package files

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"deduplicator/db"
	"deduplicator/logging"

	"github.com/lib/pq"
	"github.com/schollz/progressbar/v3"
)

func buildHashWhereClause(opts HashOptions) string {
	// Base: filter to the target hostname (case-insensitive).
	whereClause := `
		WHERE LOWER(hostname) = LOWER($1)
	`

	// If --refresh is set, we intentionally don't add any hash-related predicate.
	if !opts.Refresh {
		if opts.RetryProblematic && opts.Renew {
			whereClause += ` AND (hash IS NULL OR hash IN ('TIMEOUT_ERROR', 'HASH_ERROR') OR last_hashed_at < NOW() - INTERVAL '1 week')`
		} else if opts.RetryProblematic {
			whereClause += ` AND (hash IS NULL OR hash IN ('TIMEOUT_ERROR', 'HASH_ERROR'))`
		} else if opts.Renew {
			whereClause += ` AND (hash IS NULL OR last_hashed_at < NOW() - INTERVAL '1 week')`
		} else {
			whereClause += ` AND hash IS NULL`
		}
	}

	if !opts.FullHash {
		whereClause += `
		AND size IS NOT NULL
		AND size IN (
			SELECT size
			FROM files
			WHERE LOWER(hostname) = LOWER($1)
			AND size IS NOT NULL
			GROUP BY size
			HAVING COUNT(*) > 1
		)`
	}

	return whereClause
}

type hashBatchQueryOptions struct {
	LargeFirst      bool
	PrioritizePaths bool
}

func buildHashPathPriorityExpression(parameterIndex int) string {
	return fmt.Sprintf("COALESCE(array_position($%d::text[], COALESCE(root_folder, '')), cardinality($%d::text[]) + 1)", parameterIndex, parameterIndex)
}

func buildHashBatchQuery(whereClause string, batchSize int, opts hashBatchQueryOptions) string {
	if opts.PrioritizePaths {
		priorityExpr := buildHashPathPriorityExpression(2)
		if opts.LargeFirst {
			return fmt.Sprintf(
				`SELECT id, path, root_folder, COALESCE(size, -1) AS effective_size, %s AS path_priority
				FROM files %s
				AND (
					$3::int IS NULL
					OR %s > $3::int
					OR (
						%s = $3::int
						AND (
							$4::bigint IS NULL
							OR COALESCE(size, -1) < $4::bigint
							OR (COALESCE(size, -1) = $4::bigint AND id > $5)
						)
					)
				)
				ORDER BY path_priority ASC, COALESCE(size, -1) DESC, id ASC
				LIMIT %d`,
				priorityExpr,
				whereClause,
				priorityExpr,
				priorityExpr,
				batchSize,
			)
		}

		return fmt.Sprintf(
			`SELECT id, path, root_folder, COALESCE(size, -1) AS effective_size, %s AS path_priority
			FROM files %s
			AND (
				$3::int IS NULL
				OR %s > $3::int
				OR (%s = $3::int AND id > $4)
			)
			ORDER BY path_priority ASC, id ASC
			LIMIT %d`,
			priorityExpr,
			whereClause,
			priorityExpr,
			priorityExpr,
			batchSize,
		)
	}

	if opts.LargeFirst {
		return fmt.Sprintf(
			`SELECT id, path, root_folder, COALESCE(size, -1) AS effective_size
			FROM files %s
			AND (
				$2::bigint IS NULL
				OR COALESCE(size, -1) < $2::bigint
				OR (COALESCE(size, -1) = $2::bigint AND id > $3)
			)
			ORDER BY COALESCE(size, -1) DESC, id ASC
			LIMIT %d`,
			whereClause,
			batchSize,
		)
	}

	return fmt.Sprintf(
		`SELECT id, path, root_folder, COALESCE(size, -1) AS effective_size
		FROM files %s AND id > $2
		ORDER BY id ASC
		LIMIT %d`,
		whereClause,
		batchSize,
	)
}

func nullableInt64Value(value sql.NullInt64) interface{} {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func resolveHashPriorityRootFolders(host *db.Host, paths []string) ([]string, error) {
	priorityPaths := make([]string, 0, len(paths))
	seenPaths := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		if _, seen := seenPaths[path]; seen {
			continue
		}
		seenPaths[path] = struct{}{}
		priorityPaths = append(priorityPaths, path)
	}
	if len(priorityPaths) == 0 {
		return nil, nil
	}

	configuredPaths, err := host.GetPaths()
	if err != nil {
		return nil, fmt.Errorf("error decoding host paths: %v", err)
	}

	rootFolders := make([]string, 0, len(priorityPaths))
	seenRoots := make(map[string]struct{}, len(priorityPaths))
	for _, path := range priorityPaths {
		rootFolder, ok := configuredPaths[path]
		if !ok {
			if !filepath.IsAbs(path) {
				return nil, fmt.Errorf("friendly path '%s' not found for server '%s'", path, host.Name)
			}
			rootFolder = path
		}
		if _, seen := seenRoots[rootFolder]; seen {
			continue
		}
		seenRoots[rootFolder] = struct{}{}
		rootFolders = append(rootFolders, rootFolder)
	}

	return rootFolders, nil
}

// HashFiles calculates hashes for files in the database
func HashFiles(ctx context.Context, sqldb *sql.DB, opts HashOptions) error {
	// Get host information by hostname (case-insensitive)
	host, err := db.GetHostByHostname(sqldb, opts.Server)
	if err != nil {
		// Try by name if not found by hostname
		host, err = db.GetHost(sqldb, opts.Server)
		if err != nil {
			return fmt.Errorf("server not found: %s", opts.Server)
		}
	}
	hostname := host.Hostname
	priorityRootFolders, err := resolveHashPriorityRootFolders(host, opts.Paths)
	if err != nil {
		return err
	}

	// Build base WHERE clause (no SELECT list) based on options.
	// We batch using `id > lastID` so we don't re-process rows even if the filter
	// would still match after updating their hash (notably for --retry-problematic).
	whereClause := buildHashWhereClause(opts)
	// First, count total files to process
	var totalFiles int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM files %s", whereClause)
	err = sqldb.QueryRow(countQuery, hostname).Scan(&totalFiles)
	if err != nil {
		return fmt.Errorf("error counting files: %v", err)
	}

	if totalFiles == 0 {
		// fmt.Println("No files need hashing")
		return nil
	}

	// Create progress bar
	bar := progressbar.NewOptions64(totalFiles,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan]Processing files..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Prepare update statement
	stmt, err := sqldb.Prepare(`
		UPDATE files
		SET hash = $1, last_hashed_at = NOW()
		WHERE id = $2
	`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	// Prepare statement to mark files that timed out
	skipStmt, err := sqldb.Prepare(`
		UPDATE files
		SET hash = 'TIMEOUT_ERROR', last_hashed_at = NOW()
		WHERE id = $1
	`)
	if err != nil {
		return fmt.Errorf("error preparing skip statement: %v", err)
	}
	defer skipStmt.Close()

	// Prepare statement to mark files that errored (non-timeout)
	hashErrStmt, err := sqldb.Prepare(`
		UPDATE files
		SET hash = 'HASH_ERROR', last_hashed_at = NOW()
		WHERE id = $1
	`)
	if err != nil {
		return fmt.Errorf("error preparing hash error statement: %v", err)
	}
	defer hashErrStmt.Close()

	// Instead of querying all files at once, we'll fetch them in batches
	// to avoid keeping all file records in memory
	batchSize := 100

	// Use a keyset bookmark for batching instead of OFFSET.
	lastID := 0
	var lastEffectiveSize sql.NullInt64
	var lastPathPriority sql.NullInt64

	// Track statistics
	var processed, skipped int64

	prioritizePaths := len(priorityRootFolders) > 0
	batchQuery := buildHashBatchQuery(whereClause, batchSize, hashBatchQueryOptions{
		LargeFirst:      opts.LargeFirst,
		PrioritizePaths: prioritizePaths,
	})
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled after processing %d of %d files", processed+skipped, totalFiles)
		default:
		}

		var rows *sql.Rows
		if prioritizePaths && opts.LargeFirst {
			rows, err = sqldb.Query(batchQuery, hostname, pq.Array(priorityRootFolders), nullableInt64Value(lastPathPriority), nullableInt64Value(lastEffectiveSize), lastID)
		} else if prioritizePaths {
			rows, err = sqldb.Query(batchQuery, hostname, pq.Array(priorityRootFolders), nullableInt64Value(lastPathPriority), lastID)
		} else if opts.LargeFirst {
			rows, err = sqldb.Query(batchQuery, hostname, nullableInt64Value(lastEffectiveSize), lastID)
		} else {
			rows, err = sqldb.Query(batchQuery, hostname, lastID)
		}
		if err != nil {
			return fmt.Errorf("error querying files: %v", err)
		}

		fileCount := 0
		for rows.Next() {
			select {
			case <-ctx.Done():
				rows.Close()
				// fmt.Printf("\nOperation cancelled after processing %d files\n", processed)
				return fmt.Errorf("operation cancelled")
			default:
			}
			var id int
			var dbPath string
			var rootFolder sql.NullString
			var effectiveSize int64
			var pathPriority int64
			if prioritizePaths {
				err = rows.Scan(&id, &dbPath, &rootFolder, &effectiveSize, &pathPriority)
			} else {
				err = rows.Scan(&id, &dbPath, &rootFolder, &effectiveSize)
			}
			if err != nil {
				logging.InfoLogger.Printf("Warning: Error scanning row: %v", err)
				continue
			}

			// Update lastID to the current file's id
			lastID = id
			lastEffectiveSize = sql.NullInt64{Int64: effectiveSize, Valid: true}
			if prioritizePaths {
				lastPathPriority = sql.NullInt64{Int64: pathPriority, Valid: true}
			}
			fileCount++

			// Construct the full dbPath from root_folder + dbPath
			fullPath := filepath.Join(rootFolder.String, dbPath)

			// Display the file name before hashing
			logging.InfoLogger.Printf("Hashing file: %s", filepath.Base(dbPath))

			// Calculate hash - this will block until the hash is complete or times out
			hash, err := calculateFileHash(fullPath)
			if err != nil {
				if strings.Contains(err.Error(), "hashing timed out") || strings.Contains(err.Error(), "hashing operation cancelled") {
					logging.InfoLogger.Printf("Warning: Timeout while hashing file %s: %v", dbPath, err)
					// Mark file as problematic in the database
					_, dbErr := skipStmt.Exec(id)
					if dbErr != nil {
						logging.InfoLogger.Printf("Warning: Error marking file as problematic: %v", dbErr)
					} else {
						skipped++
						logging.InfoLogger.Printf("Marked file as problematic: %s", dbPath)
					}
				} else {
					logging.InfoLogger.Printf("Warning: Error hashing file %s: %v", dbPath, err)
					_, dbErr := hashErrStmt.Exec(id)
					if dbErr != nil {
						logging.InfoLogger.Printf("Warning: Error marking file as hash error: %v", dbErr)
					}
				}
				bar.Add(1)
				continue
			}

			// Update database
			_, err = stmt.Exec(hash, id)
			if err != nil {
				logging.InfoLogger.Printf("Warning: Error updating hash for file %s: %v", dbPath, err)
				continue
			}

			processed++
			bar.Add(1)

			// Check for context cancellation after each file
			select {
			case <-ctx.Done():
				rows.Close()
				return fmt.Errorf("operation cancelled after processing %d of %d files", processed+skipped, totalFiles)
			default:
			}
		}

		rows.Close()

		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating rows: %v", err)
		}

		if fileCount < batchSize {
			break
		}
	}

	// fmt.Printf("\nSuccessfully processed %d files\n", processed)
	if skipped > 0 {
		// fmt.Printf("Skipped %d problematic files (marked with TIMEOUT_ERROR in database)\n", skipped)
	}
	return nil
}

// ListProblematicFiles lists files that have been marked with TIMEOUT_ERROR
func ListProblematicFiles(ctx context.Context, db *sql.DB, hostname string) error {
	// Get host information
	var rootPath string
	err := db.QueryRow(`
		SELECT root_path
		FROM hosts
		WHERE LOWER(name) = LOWER($1)
	`, hostname).Scan(&rootPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("host not found: %s", hostname)
		}
		return fmt.Errorf("error getting host info: %v", err)
	}

	// Query for problematic files
	query := `
		SELECT id, dbPath, size, last_hashed_at
		FROM files
		WHERE LOWER(hostname) = LOWER($1) AND hash = 'TIMEOUT_ERROR'
		ORDER BY last_hashed_at DESC
	`

	rows, err := db.QueryContext(ctx, query, hostname)
	if err != nil {
		return fmt.Errorf("error querying problematic files: %v", err)
	}
	defer rows.Close()

	// Count the results
	var count int
	// fmt.Println("Files marked as problematic (TIMEOUT_ERROR):")
	// fmt.Println("--------------------------------------------")
	// fmt.Printf("%-10s %-20s %-15s %s\n", "ID", "Last Attempt", "Size", "Path")
	// fmt.Println("--------------------------------------------")

	for rows.Next() {
		var id int
		var dbPath string
		var size int64
		var lastHashedAt time.Time

		err := rows.Scan(&id, &dbPath, &size, &lastHashedAt)
		if err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		// Format the size in a human-readable way
		// sizeStr calculation removed as it's not used when output is suppressed
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	if count == 0 {
		// fmt.Println("No problematic files found.")
	} else {
		// fmt.Printf("\nFound %d problematic files.\n", count)
		// fmt.Println("\nTo retry these files, use: dedupe hash --retry-problematic")
	}

	return nil
}
