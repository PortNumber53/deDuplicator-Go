package files

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"deduplicator/db"
	"deduplicator/logging"

	"github.com/schollz/progressbar/v3"
)

// PruneOptions represents options for pruning files
type PruneOptions struct {
	BatchSize int // Number of deletions per transaction commit
}

// PruneNonExistentFiles removes entries for files that no longer exist
func PruneNonExistentFiles(ctx context.Context, sqldb *sql.DB, opts PruneOptions) error {
	startTime := time.Now()

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 250 // Default batch size
	}

	// Get hostname for current machine
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error getting hostname: %v", err)
	}

	// Convert hostname to lowercase for consistency
	hostname = strings.ToLower(hostname)
	logging.InfoLogger.Printf("Looking up host for hostname: %s", hostname)

	// Get host information and all paths (case-insensitive hostname lookup)
	host, err := db.GetHostByHostname(sqldb, hostname)
	if err != nil {
		return fmt.Errorf("error fetching host: %v", err)
	}

	fmt.Printf("Checking files for host '%s'...\n", host.Name)

	// First, count total files to check - use case-insensitive comparison
	var totalFiles int
	countQuery := "SELECT COUNT(*) FROM files WHERE LOWER(hostname) = LOWER($1)" + getRowLimitClause()
	err = sqldb.QueryRow(countQuery, host.Hostname).Scan(&totalFiles)
	if err != nil {
		return fmt.Errorf("error counting files: %v", err)
	}
	fmt.Printf("Found %d files to check in the database (limited for quick iteration)\n", totalFiles)

	if totalFiles == 0 {
		fmt.Println("No files to check (after applying row limit)")
		return nil
	}

	// Get files for this host - use case-insensitive comparison
	query := "SELECT id, path, root_folder FROM files WHERE LOWER(hostname) = LOWER($1)" + getRowLimitClause()
	rows, err := sqldb.Query(query, host.Hostname)
	if err != nil {
		return fmt.Errorf("error querying files: %v", err)
	}
	defer rows.Close()

	// Begin transaction for batch deletes
	tx, err := sqldb.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer func() {
		_ = tx.Rollback() // safe to call, will be ignored if already committed
	}()

	// Prepare delete statement
	stmt, err := tx.Prepare(`DELETE FROM files WHERE id = $1`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	batchDeletes := 0

	// Create progress bar
	bar := progressbar.NewOptions64(int64(totalFiles), // This now matches the limited row count
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan]Checking files..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Check each file
	var removedNonexistent, removedSymlinks, removedDevices, removedMissing int
	var checked int
	for rows.Next() {
		select {
		case <-ctx.Done():
			fmt.Printf("\nOperation cancelled after processing %d files\n", checked)
			return fmt.Errorf("operation cancelled")
		default:
		}
		var id int
		var dbPath string
		var rootFolder sql.NullString
		err := rows.Scan(&id, &dbPath, &rootFolder)
		if err != nil {
			logging.ErrorLogger.Printf("Warning: Error scanning row: %v", err)
			continue
		}

		checked++
		if checked%1000 == 0 {
			// fmt.Printf("Checked %d/%d files...\n", checked, totalFiles)
			logging.InfoLogger.Printf("Checked %d/%d files...", checked, totalFiles)
		}

		// Use root_folder to construct the full path (empty string if NULL)
		fullPath := filepath.Join(rootFolder.String, dbPath)
		fileInfo, err := os.Lstat(fullPath)
		if err != nil {
			// Could not stat the file for any reason – treat as non-existent
			_, err = stmt.Exec(id)
			if err != nil {
				logging.ErrorLogger.Printf("Warning: Error deleting file %s: %v", dbPath, err)
				bar.Add(1)
				continue
			}
			removedNonexistent++
			batchDeletes++
			logging.InfoLogger.Printf("Deleted entry for non-existent or invalid file: %s", dbPath)
			if batchDeletes >= batchSize {
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("error committing transaction: %v", err)
				}
				logging.InfoLogger.Printf("Committed batch of %d deletions", batchDeletes)
				tx, err = sqldb.Begin()
				if err != nil {
					return fmt.Errorf("error starting new transaction: %v", err)
				}
				stmt, err = tx.Prepare(`DELETE FROM files WHERE id = $1`)
				if err != nil {
					return fmt.Errorf("error preparing statement: %v", err)
				}
				batchDeletes = 0
			}
			bar.Add(1)
			continue
		}

		// Check for symlinks
		if fileInfo.Mode()&os.ModeSymlink != 0 {
			// Delete symlinks from database
			_, err = stmt.Exec(id)
			if err != nil {
				logging.ErrorLogger.Printf("Warning: Error deleting symlink %s: %v", dbPath, err)
				continue
			}
			removedSymlinks++
			batchDeletes++
			logging.InfoLogger.Printf("Deleted entry for symlink: %s", fullPath)
			if batchDeletes >= batchSize {
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("error committing transaction: %v", err)
				}
				logging.InfoLogger.Printf("Committed batch of %d deletions", batchDeletes)
				tx, err = sqldb.Begin()
				if err != nil {
					return fmt.Errorf("error starting new transaction: %v", err)
				}
				stmt, err = tx.Prepare(`DELETE FROM files WHERE id = $1`)
				if err != nil {
					return fmt.Errorf("error preparing statement: %v", err)
				}
				batchDeletes = 0
			}
		}

		// Check for device files, pipes, sockets, etc.
		if fileInfo.Mode()&(os.ModeDevice|os.ModeCharDevice|os.ModeNamedPipe|os.ModeSocket) != 0 {
			// Delete device files from database
			_, err = stmt.Exec(id)
			if err != nil {
				logging.ErrorLogger.Printf("Warning: Error deleting device file %s: %v", dbPath, err)
				continue
			}
			removedDevices++
			batchDeletes++
			logging.InfoLogger.Printf("Deleted entry for device file: %s", fullPath)
			if batchDeletes >= batchSize {
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("error committing transaction: %v", err)
				}
				logging.InfoLogger.Printf("Committed batch of %d deletions", batchDeletes)
				tx, err = sqldb.Begin()
				if err != nil {
					return fmt.Errorf("error starting new transaction: %v", err)
				}
				stmt, err = tx.Prepare(`DELETE FROM files WHERE id = $1`)
				if err != nil {
					return fmt.Errorf("error preparing statement: %v", err)
				}
				batchDeletes = 0
			}
		}

		bar.Add(1)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	// Commit any remaining deletions
	if batchDeletes > 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("error committing final transaction: %v", err)
		}
		logging.InfoLogger.Printf("Committed final batch of %d deletions", batchDeletes)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\nChecked %d files in total\n", checked)
	fmt.Printf("Removed %d entries for non-existent files\n", removedNonexistent)
	fmt.Printf("Removed %d entries for symlinks\n", removedSymlinks)
	fmt.Printf("Removed %d entries for device files\n", removedDevices)
	fmt.Printf("Removed %d entries for missing friendly paths\n", removedMissing)
	fmt.Printf("Wall time: %s\n", elapsed.Round(time.Millisecond))
	return nil
}
