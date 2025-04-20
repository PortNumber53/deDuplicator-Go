package files

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	// Use the friendly path as the subdirectory under root_path
	if opts.FriendlyPath == "" {
		return fmt.Errorf("friendly path is required")
	}
	// Ensure root path ends with a slash
	if !strings.HasSuffix(rootPath, "/") {
		rootPath += "/"
	}
	// Ensure friendly path does not start with a slash
	friendlyPath := strings.TrimLeft(opts.FriendlyPath, "/")

	// Avoid duplicating friendlyPath if rootPath already ends with it (case-insensitive, with or without trailing slash)
	var destRoot string
	rootLower := strings.ToLower(strings.TrimRight(rootPath, "/"))
	friendlyLower := strings.ToLower(strings.Trim(friendlyPath, "/"))
	if strings.HasSuffix(rootLower, "/"+friendlyLower) || rootLower == friendlyLower {
		destRoot = rootPath
	} else {
		destRoot = filepath.Join(rootPath, friendlyPath) + "/"
	}

	fmt.Printf("Importing files from %s to %s (%s:%s)\n", opts.SourcePath, targetHost, targetHost, destRoot)
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
				return nil
			}

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
				}
				fmt.Printf("Transferring %s to %s\n", path, targetPath)
			} else {
				rsyncArgs = []string{"-avz", path, targetHost+":"+targetPath}
				if opts.RemoveSource {
					rsyncArgs = []string{"-avz", "--remove-source-files", path, targetHost+":"+targetPath}
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

	fmt.Printf("\nImport summary:\n")
	fmt.Printf("  Total files processed: %d\n", fileCount)
	fmt.Printf("  Files transferred: %d\n", transferCount)
	fmt.Printf("  Files skipped (already exist): %d\n", skipCount)
	fmt.Printf("  Errors: %d\n", errorCount)

	return nil
}
