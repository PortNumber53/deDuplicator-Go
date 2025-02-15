package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/schollz/progressbar/v3"
)

// FindOptions represents options for the find command
type FindOptions struct {
	Host string
}

// FindFiles traverses the root path of the specified host and adds files to the database
func FindFiles(ctx context.Context, db *sql.DB, opts FindOptions) error {
	// Get host information
	var rootPath, hostname string
	log.Printf("Looking up host information for host: %s", opts.Host)
	err := db.QueryRow(`
		SELECT root_path, hostname
		FROM hosts 
		WHERE name = $1
	`, opts.Host).Scan(&rootPath, &hostname)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("host not found: %s", opts.Host)
		}
		return fmt.Errorf("error getting host info: %v", err)
	}
	log.Printf("Found root path: %s", rootPath)

	// Ensure root path exists
	if _, err := os.Stat(rootPath); os.IsNotExist(err) {
		return fmt.Errorf("root path does not exist: %s", rootPath)
	}
	log.Printf("Root path exists and is accessible")

	// Track processed files
	var processedFiles int64
	var currentBatch int64
	var tx *sql.Tx
	var stmt *sql.Stmt

	// Function to start a new transaction
	startNewTransaction := func() error {
		// If we have an existing transaction, commit it
		if tx != nil {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("error committing transaction: %v", err)
			}
			stmt.Close()
		}

		// Start new transaction
		tx, err = db.Begin()
		if err != nil {
			return fmt.Errorf("error starting transaction: %v", err)
		}

		// Prepare statement for batch inserts
		stmt, err = tx.Prepare(`
			INSERT INTO files (path, hostname, size)
			VALUES ($1, $2, $3)
			ON CONFLICT (path, hostname) 
			DO UPDATE SET size = EXCLUDED.size
		`)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error preparing statement: %v", err)
		}

		currentBatch = 0
		return nil
	}

	// Start initial transaction
	if err := startNewTransaction(); err != nil {
		return err
	}

	// Create progress bar (indeterminate)
	bar := progressbar.NewOptions(-1,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan]Finding files..."),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Walk the directory tree
	err = filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			log.Printf("Warning: Error accessing path %s: %v", path, err)
			return nil
		}

		// Skip directories and symlinks
		if info.IsDir() || (info.Mode()&os.ModeSymlink) != 0 {
			return nil
		}

		// Get relative path from root
		relPath, err := filepath.Rel(rootPath, path)
		if err != nil {
			log.Printf("Warning: Error getting relative path for %s: %v", path, err)
			return nil
		}

		// Insert file into database using hostname instead of host name
		_, err = stmt.Exec(relPath, hostname, info.Size())
		if err != nil {
			log.Printf("Warning: Error inserting file %s: %v", relPath, err)
			return nil
		}

		processedFiles++
		currentBatch++

		// Commit every 1000 files
		if currentBatch >= 1000 {
			if err := startNewTransaction(); err != nil {
				return err
			}
			log.Printf("Processed and committed %d files so far...", processedFiles)
		}

		bar.Add(1)
		return nil
	})

	if err != nil {
		if err == context.Canceled {
			// Try to commit the last batch before returning
			if tx != nil {
				if err := tx.Commit(); err != nil {
					log.Printf("Warning: Error committing final batch: %v", err)
				} else {
					log.Printf("Successfully committed final batch")
				}
			}
			fmt.Printf("\nOperation cancelled after processing %d files\n", processedFiles)
			return fmt.Errorf("operation cancelled")
		}
		return fmt.Errorf("error walking directory: %v", err)
	}

	// Commit final transaction if there are any remaining files
	if currentBatch > 0 && tx != nil {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("error committing final transaction: %v", err)
		}
	}

	fmt.Printf("\nSuccessfully processed %d files\n", processedFiles)
	return nil
}
