package files

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ImportFiles imports files from a source directory to a target host
func ImportFiles(ctx context.Context, database *sql.DB, opts ImportOptions) error {
	// Validate options
	if opts.SourcePath == "" {
		return fmt.Errorf("source path is required")
	}
	if opts.HostName == "" {
		return fmt.Errorf("host name is required")
	}

	// Check if source path exists
	sourceInfo, err := os.Stat(opts.SourcePath)
	if err != nil {
		return fmt.Errorf("error accessing source path: %v", err)
	}
	if !sourceInfo.IsDir() {
		return fmt.Errorf("source path must be a directory")
	}

	// Get host information from database
	var name, ip, rootPath string
	err = database.QueryRow(`
		SELECT name, ip, root_path
		FROM hosts
		WHERE LOWER(hostname) = LOWER($1)
	`, opts.HostName).Scan(&name, &ip, &rootPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("host '%s' not found in database", opts.HostName)
		}
		return fmt.Errorf("error querying host information: %v", err)
	}

	// Ensure root path ends with a slash
	if !strings.HasSuffix(rootPath, "/") {
		rootPath += "/"
	}

	fmt.Printf("Importing files from %s to %s (%s:%s)\n", opts.SourcePath, opts.HostName, name, rootPath)
	if opts.DryRun {
		fmt.Println("DRY RUN: No files will be transferred or removed")
	}

	// Walk through the source directory
	fileCount := 0
	transferCount := 0
	skipCount := 0
	errorCount := 0
	processedCount := 0

	err = filepath.Walk(opts.SourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error accessing path %s: %v\n", path, err)
			errorCount++
			return nil
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check if we've reached the count limit
		if opts.Count > 0 && processedCount >= opts.Count {
			return filepath.SkipAll
		}

		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fileCount++
		processedCount++

		// Calculate file hash
		hash, err := calculateFileHash(path)
		if err != nil {
			fmt.Printf("Error calculating hash for %s: %v\n", path, err)
			errorCount++
			return nil
		}

		// Check if file with this hash already exists for this host
		var existingCount int
		err = database.QueryRow(`
			SELECT COUNT(*)
			FROM files
			WHERE hash = $1 AND hostname = $2
		`, hash, opts.HostName).Scan(&existingCount)
		if err != nil {
			fmt.Printf("Error querying database for hash %s: %v\n", hash, err)
			errorCount++
			return nil
		}

		if existingCount > 0 {
			fmt.Printf("Skipping %s (hash already exists on target host)\n", path)
			skipCount++
			return nil
		}

		// Get relative path from source directory
		relPath, err := filepath.Rel(opts.SourcePath, path)
		if err != nil {
			fmt.Printf("Error getting relative path for %s: %v\n", path, err)
			errorCount++
			return nil
		}

		// Construct target path
		targetPath := filepath.Join(rootPath, relPath)

		if !opts.DryRun {
			// Create target directory structure first
			targetDir := filepath.Dir(targetPath)
			mkdirCmd := exec.CommandContext(ctx, "ssh", name, "mkdir", "-p", targetDir)
			if err := mkdirCmd.Run(); err != nil {
				fmt.Printf("Error creating directory %s: %v\n", targetDir, err)
				errorCount++
				return nil
			}

			// Use rsync to transfer the file (without --mkpath which isn't supported in older rsync)
			rsyncArgs := []string{"-avz", path, name+":"+targetPath}
			if opts.RemoveSource {
				rsyncArgs = append([]string{"-avz", "--remove-source-files"}, path, name+":"+targetPath)
			}
			rsyncCmd := exec.CommandContext(ctx, "rsync", rsyncArgs...)
			fmt.Printf("Transferring %s to %s:%s\n", path, name, targetPath)

			output, err := rsyncCmd.CombinedOutput()
			if err != nil {
				fmt.Printf("Error transferring file %s: %v\n%s\n", path, err, output)
				errorCount++
				return nil
			}

			// Add file to database
			_, err = database.Exec(`
				INSERT INTO files (path, size, hash, hostname)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (path, hostname) DO UPDATE
				SET size = $2, hash = $3
			`, targetPath, info.Size(), hash, opts.HostName)
			if err != nil {
				fmt.Printf("Error adding file to database: %v\n", err)
				errorCount++
				return nil
			}
		} else {
			fmt.Printf("Would transfer %s to %s:%s\n", path, name, targetPath)
			if opts.RemoveSource {
				fmt.Printf("Would remove source file %s after transfer\n", path)
			}
		}

		transferCount++
		return nil
	})

	if err != nil {
		return fmt.Errorf("error walking source directory: %v", err)
	}

	fmt.Printf("\nImport summary:\n")
	fmt.Printf("  Total files processed: %d\n", fileCount)
	fmt.Printf("  Files transferred: %d\n", transferCount)
	fmt.Printf("  Files skipped (already exist): %d\n", skipCount)
	fmt.Printf("  Errors: %d\n", errorCount)

	return nil
}
