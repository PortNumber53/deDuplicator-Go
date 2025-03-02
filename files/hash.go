package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
)

// HashFiles calculates hashes for files in the database
func HashFiles(ctx context.Context, db *sql.DB, opts HashOptions) error {
	// Get host information
	var rootPath, hostname string
	err := db.QueryRow(`
		SELECT root_path, hostname
		FROM hosts 
		WHERE LOWER(name) = LOWER($1)
	`, opts.Host).Scan(&rootPath, &hostname)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("host not found: %s", opts.Host)
		}
		return fmt.Errorf("error getting host info: %v", err)
	}

	// Build query based on options
	query := `
		SELECT id, path 
		FROM files 
		WHERE LOWER(hostname) = LOWER($1)
	`
	if !opts.Refresh {
		if opts.RetryProblematic && opts.Renew {
			query += ` AND (hash IS NULL OR hash = 'TIMEOUT_ERROR' OR last_hashed_at < NOW() - INTERVAL '1 week')`
		} else if opts.RetryProblematic {
			query += ` AND (hash IS NULL OR hash = 'TIMEOUT_ERROR')`
		} else if opts.Renew {
			query += ` AND (hash IS NULL OR last_hashed_at < NOW() - INTERVAL '1 week')`
		} else {
			query += ` AND hash IS NULL`
		}
	}

	// First, count total files to process
	var totalFiles int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS subquery", query)
	err = db.QueryRow(countQuery, hostname).Scan(&totalFiles)
	if err != nil {
		return fmt.Errorf("error counting files: %v", err)
	}

	if totalFiles == 0 {
		fmt.Println("No files need hashing")
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
	stmt, err := db.Prepare(`
		UPDATE files 
		SET hash = $1, last_hashed_at = NOW()
		WHERE id = $2
	`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	// Prepare statement to mark problematic files
	skipStmt, err := db.Prepare(`
		UPDATE files 
		SET hash = 'TIMEOUT_ERROR', last_hashed_at = NOW()
		WHERE id = $1
	`)
	if err != nil {
		return fmt.Errorf("error preparing skip statement: %v", err)
	}
	defer skipStmt.Close()

	// Instead of querying all files at once, we'll fetch them in batches
	// to avoid keeping all file records in memory
	batchSize := 100
	offset := 0

	// Track statistics
	var processed, skipped int64

	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled after processing %d of %d files", processed+skipped, totalFiles)
		default:
		}

		// Query a batch of files
		batchQuery := fmt.Sprintf("%s LIMIT %d OFFSET %d", query, batchSize, offset)
		rows, err := db.Query(batchQuery, hostname)
		if err != nil {
			return fmt.Errorf("error querying files: %v", err)
		}

		// Process each file in the batch
		fileCount := 0
		for rows.Next() {
			fileCount++

			var id int
			var path string
			err := rows.Scan(&id, &path)
			if err != nil {
				log.Printf("Warning: Error scanning row: %v", err)
				continue
			}

			// Construct full path
			fullPath := filepath.Join(rootPath, path)

			// Display the file name before hashing
			fmt.Printf("\nHashing file: %s\n", filepath.Base(path))

			// Calculate hash - this will block until the hash is complete or times out
			hash, err := calculateFileHash(fullPath)
			if err != nil {
				if strings.Contains(err.Error(), "hashing timed out") || strings.Contains(err.Error(), "hashing operation cancelled") {
					log.Printf("Warning: Timeout while hashing file %s: %v", path, err)
					// Mark file as problematic in the database
					_, dbErr := skipStmt.Exec(id)
					if dbErr != nil {
						log.Printf("Warning: Error marking file as problematic: %v", dbErr)
					} else {
						skipped++
						log.Printf("Marked file as problematic: %s", path)
					}
				} else {
					log.Printf("Warning: Error hashing file %s: %v", path, err)
				}
				bar.Add(1)
				continue
			}

			// Update database
			_, err = stmt.Exec(hash, id)
			if err != nil {
				log.Printf("Warning: Error updating hash for file %s: %v", path, err)
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

		// If we got fewer files than the batch size, we're done
		if fileCount < batchSize {
			break
		}

		// Move to the next batch
		offset += batchSize
	}

	fmt.Printf("\nSuccessfully processed %d files\n", processed)
	if skipped > 0 {
		fmt.Printf("Skipped %d problematic files (marked with TIMEOUT_ERROR in database)\n", skipped)
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
		SELECT id, path, size, last_hashed_at
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
	fmt.Println("Files marked as problematic (TIMEOUT_ERROR):")
	fmt.Println("--------------------------------------------")
	fmt.Printf("%-10s %-20s %-15s %s\n", "ID", "Last Attempt", "Size", "Path")
	fmt.Println("--------------------------------------------")

	for rows.Next() {
		var id int
		var path string
		var size int64
		var lastHashedAt time.Time

		err := rows.Scan(&id, &path, &size, &lastHashedAt)
		if err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		// Format the size in a human-readable way
		sizeStr := fmt.Sprintf("%d bytes", size)
		if size > 1024*1024*1024 {
			sizeStr = fmt.Sprintf("%.2f GB", float64(size)/(1024*1024*1024))
		} else if size > 1024*1024 {
			sizeStr = fmt.Sprintf("%.2f MB", float64(size)/(1024*1024))
		} else if size > 1024 {
			sizeStr = fmt.Sprintf("%.2f KB", float64(size)/1024)
		}

		fmt.Printf("%-10d %-20s %-15s %s\n", id, lastHashedAt.Format("2006-01-02 15:04:05"), sizeStr, path)
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	if count == 0 {
		fmt.Println("No problematic files found.")
	} else {
		fmt.Printf("\nFound %d problematic files.\n", count)
		fmt.Println("\nTo retry these files, use: dedupe hash --retry-problematic")
	}

	return nil
}
