package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/schollz/progressbar/v3"
)

// PruneNonExistentFiles removes entries for files that no longer exist
func PruneNonExistentFiles(ctx context.Context, db *sql.DB, opts PruneOptions) error {
	// Get hostname for current machine
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error getting hostname: %v", err)
	}

	// Convert hostname to lowercase for consistency
	hostname = strings.ToLower(hostname)
	log.Printf("Looking up host for hostname: %s", hostname)

	// Find host in database by hostname (case-insensitive)
	var hostName string
	err = db.QueryRow(`
		SELECT name
		FROM hosts
		WHERE LOWER(hostname) = LOWER($1)
	`, hostname).Scan(&hostName)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add'", hostname)
		}
		return fmt.Errorf("error finding host: %v", err)
	}
	log.Printf("Found host: %s", hostName)

	// Get host information
	var host struct {
		name     string
		rootPath string
	}

	err = db.QueryRow("SELECT name, root_path FROM hosts WHERE name = $1", hostName).Scan(&host.name, &host.rootPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("host not found: %s", hostName)
		}
		return fmt.Errorf("error querying host: %v", err)
	}

	fmt.Printf("Checking files for host '%s'...\n", host.name)

	// First, count total files to check - use case-insensitive comparison
	var totalFiles int
	err = db.QueryRow("SELECT COUNT(*) FROM files WHERE LOWER(hostname) = LOWER($1)", host.name).Scan(&totalFiles)
	if err != nil {
		return fmt.Errorf("error counting files: %v", err)
	}
	fmt.Printf("Found %d files to check in the database\n", totalFiles)

	if totalFiles == 0 {
		fmt.Println("No files to check")
		return nil
	}

	// Get files for this host - use case-insensitive comparison
	query := `
		SELECT id, path 
		FROM files 
		WHERE LOWER(hostname) = LOWER($1)
	`
	rows, err := db.Query(query, host.name)
	if err != nil {
		return fmt.Errorf("error querying files: %v", err)
	}
	defer rows.Close()

	// Begin transaction for batch deletes
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Prepare delete statement
	stmt, err := tx.Prepare(`DELETE FROM files WHERE id = $1`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	// Create progress bar
	bar := progressbar.NewOptions64(int64(totalFiles),
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
	var deleted, deletedSymlinks, deletedDevices, checked int
	for rows.Next() {
		var id int
		var path string
		err := rows.Scan(&id, &path)
		if err != nil {
			log.Printf("Warning: Error scanning row: %v", err)
			continue
		}

		checked++
		if checked%1000 == 0 {
			fmt.Printf("Checked %d/%d files...\n", checked, totalFiles)
		}

		// Check if file exists by joining root path with relative path
		fullPath := filepath.Join(host.rootPath, path)
		fileInfo, err := os.Lstat(fullPath)

		// Check for non-existent files
		if err != nil {
			if os.IsNotExist(err) {
				// Delete from database
				_, err = stmt.Exec(id)
				if err != nil {
					log.Printf("Warning: Error deleting file %s: %v", path, err)
					continue
				}
				deleted++
				log.Printf("Deleted entry for non-existent file: %s", fullPath)
			} else {
				log.Printf("Warning: Error checking file %s: %v", fullPath, err)
			}
			bar.Add(1)
			continue
		}

		// Check for symlinks
		if fileInfo.Mode()&os.ModeSymlink != 0 {
			// Delete symlinks from database
			_, err = stmt.Exec(id)
			if err != nil {
				log.Printf("Warning: Error deleting symlink %s: %v", path, err)
				continue
			}
			deletedSymlinks++
			log.Printf("Deleted entry for symlink: %s", fullPath)
		}

		// Check for device files, pipes, sockets, etc.
		if fileInfo.Mode()&(os.ModeDevice|os.ModeCharDevice|os.ModeNamedPipe|os.ModeSocket) != 0 {
			// Delete device files from database
			_, err = stmt.Exec(id)
			if err != nil {
				log.Printf("Warning: Error deleting device file %s: %v", path, err)
				continue
			}
			deletedDevices++
			log.Printf("Deleted entry for device file: %s", fullPath)
		}

		bar.Add(1)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	fmt.Printf("\nChecked %d files in total\n", checked)
	fmt.Printf("Removed %d entries for non-existent files\n", deleted)
	fmt.Printf("Removed %d entries for symlinks\n", deletedSymlinks)
	fmt.Printf("Removed %d entries for device files\n", deletedDevices)
	return nil
}
