package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"

	"github.com/schollz/progressbar/v3"
)

// HashOptions represents options for the hash command
type HashOptions struct {
	Host    string
	Refresh bool // hash all files regardless of existing hash
	Renew   bool // hash files with hashes older than 1 week
}

// HashFiles calculates hashes for files in the database
func HashFiles(ctx context.Context, db *sql.DB, opts HashOptions) error {
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

	// Build query based on options
	query := `
		SELECT id, path 
		FROM files 
		WHERE host = $1
	`
	if !opts.Refresh {
		if opts.Renew {
			query += ` AND (hash IS NULL OR last_hashed_at < NOW() - INTERVAL '1 week')`
		} else {
			query += ` AND hash IS NULL`
		}
	}

	// First, count total files to process
	var totalFiles int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS subquery", query)
	err = db.QueryRow(countQuery, opts.Host).Scan(&totalFiles)
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

	// Query files to process
	rows, err := db.Query(query, opts.Host)
	if err != nil {
		return fmt.Errorf("error querying files: %v", err)
	}
	defer rows.Close()

	// Process each file
	for rows.Next() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled after processing %d of %d files", bar.State().CurrentBytes, totalFiles)
		default:
		}

		var id int
		var path string
		err := rows.Scan(&id, &path)
		if err != nil {
			log.Printf("Warning: Error scanning row: %v", err)
			continue
		}

		// Construct full path
		fullPath := filepath.Join(rootPath, path)

		// Calculate hash
		hash, err := calculateFileHash(fullPath)
		if err != nil {
			log.Printf("Warning: Error hashing file %s: %v", path, err)
			continue
		}

		// Update database
		_, err = stmt.Exec(hash, id)
		if err != nil {
			log.Printf("Warning: Error updating hash for file %s: %v", path, err)
			continue
		}

		bar.Add(1)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	fmt.Printf("\nSuccessfully processed %d files\n", totalFiles)
	return nil
}
