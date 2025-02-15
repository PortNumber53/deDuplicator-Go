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
	var rootPath string
	err := db.QueryRow(`
		SELECT root_path 
		FROM hosts 
		WHERE name = $1
	`, opts.Host).Scan(&rootPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("host not found: %s", opts.Host)
		}
		return fmt.Errorf("error getting host info: %v", err)
	}

	// Ensure root path exists
	if _, err := os.Stat(rootPath); os.IsNotExist(err) {
		return fmt.Errorf("root path does not exist: %s", rootPath)
	}

	// First pass: count total files for progress bar
	var totalFiles int64
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
		if !info.IsDir() && (info.Mode()&os.ModeSymlink) == 0 {
			totalFiles++
		}
		return nil
	})
	if err != nil {
		if err == context.Canceled {
			return fmt.Errorf("file counting cancelled")
		}
		return fmt.Errorf("error counting files: %v", err)
	}

	// Begin transaction for batch inserts
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Prepare statement for batch inserts
	stmt, err := tx.Prepare(`
		INSERT INTO files (path, host, size)
		VALUES ($1, $2, $3)
		ON CONFLICT (path, host) 
		DO UPDATE SET size = EXCLUDED.size
	`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

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

	// Track processed files for partial completion message
	var processedFiles int64

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

		// Insert file into database
		_, err = stmt.Exec(relPath, opts.Host, info.Size())
		if err != nil {
			log.Printf("Warning: Error inserting file %s: %v", relPath, err)
			return nil
		}

		processedFiles++
		bar.Add(1)
		return nil
	})

	if err != nil {
		if err == context.Canceled {
			fmt.Printf("\nOperation cancelled after processing %d of %d files\n", processedFiles, totalFiles)
			return fmt.Errorf("operation cancelled")
		}
		return fmt.Errorf("error walking directory: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	fmt.Printf("\nSuccessfully processed %d files\n", totalFiles)
	return nil
}
