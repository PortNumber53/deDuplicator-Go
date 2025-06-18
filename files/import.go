package files

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"deduplicator/db"
	"deduplicator/logging"
)

// shellEscape safely quotes a string for use in a shell command (basic, single-quote style)
func shellEscape(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

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
	var displayName, ip, rootPath, dbHostName string
	err = database.QueryRow(`
		SELECT name, ip, root_path
		FROM hosts
		WHERE LOWER(name) = LOWER($1)
	`, opts.HostName).Scan(&displayName, &ip, &rootPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("host not found: %s", opts.HostName)
		}
		return fmt.Errorf("error querying host: %v", err)
	}

	// Try to get the actual hostname for the target from the hosts table
	err = database.QueryRow(`SELECT hostname FROM hosts WHERE LOWER(name) = LOWER($1)`, opts.HostName).Scan(&dbHostName)
	if err != nil || dbHostName == "" {
		// fallback to opts.HostName if not present
		dbHostName = opts.HostName
	}

	// Get the current machine's hostname
	localHost, _ := os.Hostname()
	localHost = strings.ToLower(localHost)
	targetHost := strings.ToLower(dbHostName)
	isLocal := (localHost == targetHost)

	// Get the host's path mappings
	var host db.Host
	err = database.QueryRow(`
		SELECT id, name, hostname, root_path, settings 
		FROM hosts 
		WHERE LOWER(name) = LOWER($1)
	`, opts.HostName).Scan(
		&host.ID, &host.Name, &host.Hostname, &host.RootPath, &host.Settings,
	)
	if err != nil {
		return fmt.Errorf("error getting host details: %v", err)
	}

	// Get the actual path for the friendly name
	paths, err := host.GetPaths()
	if err != nil {
		return fmt.Errorf("error getting path mappings: %v", err)
	}

	// Look up the actual path for the friendly name
	actualPath, exists := paths[opts.FriendlyPath]
	if !exists {
		// If no mapping exists, fall back to the old behavior for backward compatibility
		actualPath = filepath.Join(host.RootPath, opts.FriendlyPath)
		fmt.Printf("Warning: No path mapping found for friendly name '%s', using default path: %s\n",
			opts.FriendlyPath, actualPath)
	}

	// Use the actual path from the mapping
	destRoot := actualPath
	if !strings.HasSuffix(destRoot, "/") {
		destRoot += "/"
	}

	fmt.Printf("Importing files from %s to %s (%s:%s)\n", opts.SourcePath, targetHost, targetHost, destRoot)
	if opts.DryRun {
		fmt.Println("DRY RUN: No files will be transferred or removed")
	}

	// Walk through the source directory
	var (
		transferCount     int
		transferTotalSize int64 // Total size of transferred files
		skipCount         int
		skipTotalSize     int64 // Total size of skipped files
		errorCount        int
		fileCount         int
		removedCount      int   // Track number of files removed from source
		moveCount         int   // Track number of files moved to duplicate dir
		moveTotalSize     int64 // Total size of files moved to duplicate dir
	)

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
		if opts.Count > 0 && fileCount >= opts.Count {
			return filepath.SkipAll
		}

		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fileCount++

		// Get relative path from source directory
		relPath, err := filepath.Rel(opts.SourcePath, path)
		if err != nil {
			fmt.Printf("Error getting relative path for %s: %v\n", path, err)
			errorCount++
			return nil
		}

		// Construct target path
		targetPath := filepath.Join(destRoot, relPath)

		// Check if target file exists
		targetExists := false
		if isLocal {
			if _, err := os.Stat(targetPath); err == nil {
				targetExists = true
			}
		} else {
			// For remote, use ssh to check existence
			sshCmd := exec.CommandContext(ctx, "ssh", targetHost, "test -e "+shellEscape(targetPath))
			if err := sshCmd.Run(); err == nil {
				targetExists = true
			}
		}

		// Handle duplicate files if DuplicateDir is specified
		if targetExists && opts.DuplicateDir != "" {
			// Create the duplicate directory path by appending the relative path
			duplicatePath := filepath.Join(opts.DuplicateDir, relPath)
			duplicateDir := filepath.Dir(duplicatePath)

			if opts.DryRun {
				fmt.Printf("Would move duplicate %s to %s\n", path, duplicatePath)
			} else {
				// Create the target directory structure
				if err := os.MkdirAll(duplicateDir, 0755); err != nil {
					fmt.Printf("Error creating duplicate directory %s: %v\n", duplicateDir, err)
					errorCount++
					return nil
				}

				// Move the file to the duplicate directory using rsync for cross-filesystem moves
				err = os.Rename(path, duplicatePath)
				if err != nil {
					// If rename fails due to cross-device link, use rsync
					if strings.Contains(err.Error(), "invalid cross-device link") {
						cmd := exec.Command("rsync", "-a", "--remove-source-files", path, duplicatePath)
						output, rsyncErr := cmd.CombinedOutput()
						if rsyncErr != nil {
							fmt.Printf("Error moving duplicate file %s with rsync: %v\nOutput: %s\n", path, rsyncErr, output)
							errorCount++
							return nil
						}
					} else {
						// If it's another error, log it and continue
						fmt.Printf("Error moving duplicate file %s: %v\n", path, err)
						errorCount++
						return nil
					}
				}

				fmt.Printf("Moved duplicate %s to %s\n", path, duplicatePath)
			}
			moveCount++
			moveTotalSize += info.Size()
			return nil
		}

		if opts.DryRun {
			if targetExists {
				fmt.Printf("SKIP (target exists): %s\n", targetPath)
			} else if isLocal {
				fmt.Printf("Would transfer %s to %s\n", path, targetPath)
			} else {
				fmt.Printf("Would transfer %s to %s:%s\n", path, targetHost, targetPath)
			}
			if opts.RemoveSource && !targetExists {
				fmt.Printf("Would remove source file %s after transfer\n", path)
			}
		} else {
			if targetExists {
				fmt.Printf("SKIP (target exists): %s\n", targetPath)
				skipCount++
				skipTotalSize += info.Size()
				return nil
			}

			// Calculate file hash
			hash, err := calculateFileHash(path)
			if err != nil {
				fmt.Printf("Error calculating hash for %s: %v\n", path, err)
				errorCount++
				return nil
			}
			transferTotalSize += info.Size()

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

			// Create target directory structure first
			targetDir := filepath.Dir(targetPath)
			if isLocal {
				if err := os.MkdirAll(targetDir, 0755); err != nil {
					fmt.Printf("Error creating directory %s: %v\n", targetDir, err)
					errorCount++
					return nil
				}
			} else {
				mkdirCmd := exec.CommandContext(ctx, "ssh", targetHost, "mkdir", "-p", targetDir)
				if err := mkdirCmd.Run(); err != nil {
					fmt.Printf("Error creating directory %s: %v\n", targetDir, err)
					errorCount++
					return nil
				}
			}

			// Use rsync to transfer the file
			var rsyncArgs []string
			if isLocal {
				rsyncArgs = []string{"-avz", path, targetPath}
				if opts.RemoveSource {
					rsyncArgs = []string{"-avz", "--remove-source-files", path, targetPath}
					removedCount++
					fmt.Printf("Removed source file: %s\n", path)
				}
				fmt.Printf("Transferring %s to %s\n", path, targetPath)
			} else {
				rsyncArgs = []string{"-avz", path, targetHost + ":" + targetPath}
				if opts.RemoveSource {
					rsyncArgs = []string{"-avz", "--remove-source-files", path, targetHost + ":" + targetPath}
					removedCount++
					fmt.Printf("Removed source file: %s\n", path)
				}
				fmt.Printf("Transferring %s to %s:%s\n", path, targetHost, targetPath)
			}

			rsyncCmd := exec.CommandContext(ctx, "rsync", rsyncArgs...)

			output, err := rsyncCmd.CombinedOutput()
			if err != nil {
				fmt.Printf("Error transferring file %s: %v\n%s\n", path, err, output)
				errorCount++
				return nil
			}

			// Debug output: print query and parameters with canonical hostname
			logging.InfoLogger.Printf("INSERT INTO files (path, size, hash, hostname) VALUES ('%s', %d, '%s', '%s')", targetPath, info.Size(), hash, dbHostName)
			// Add file to database using canonical hostname
			_, err = database.Exec(`
				INSERT INTO files (path, size, hash, hostname)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (path, hostname) DO UPDATE
				SET size = $2, hash = $3
			`, targetPath, info.Size(), hash, dbHostName)
			if err != nil {
				logging.ErrorLogger.Printf("Error adding file to database: %v", err)
				errorCount++
				return nil
			}
		}

		transferCount++
		return nil
	})

	if err != nil {
		return fmt.Errorf("error walking source directory: %v", err)
	}

	// Helper function to format file sizes
	formatSize := func(size int64) string {
		const unit = 1024
		if size < unit {
			return fmt.Sprintf("%d B", size)
		}
		div, exp := int64(unit), 0
		for n := size / unit; n >= unit; n /= unit {
			div *= unit
			exp++
		}
		return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
	}

	fmt.Printf("\nImport summary:\n")
	fmt.Printf("  Total files processed: %d\n", fileCount)
	fmt.Printf("  Files transferred: %d (%s)\n", transferCount, formatSize(transferTotalSize))
	if moveCount > 0 {
		fmt.Printf("  Files moved to duplicates: %d (%s)\n", moveCount, formatSize(moveTotalSize))
	}
	if skipCount > 0 {
		fmt.Printf("  Files skipped (already exist): %d (%s)\n", skipCount, formatSize(skipTotalSize))
	}
	if opts.RemoveSource {
		fmt.Printf("  Source files removed: %d\n", removedCount)
	}
	if errorCount > 0 {
		fmt.Printf("  Errors: %d\n", errorCount)
	}

	return nil
}
