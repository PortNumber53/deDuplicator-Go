package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// MoveDuplicates moves duplicate files to a target directory
func MoveDuplicates(ctx context.Context, db *sql.DB, opts DuplicateListOptions, moveOpts MoveOptions) error {
	// Create target directory if it doesn't exist
	if !moveOpts.DryRun {
		if err := os.MkdirAll(moveOpts.TargetDir, 0755); err != nil {
			return fmt.Errorf("error creating target directory: %v", err)
		}
	}

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
		SELECT hostname
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

	// Build query based on options
	query := `
		WITH duplicates AS (
			SELECT hash, COUNT(*) as count, SUM(size) as total_size
			FROM files
			WHERE hash IS NOT NULL
			AND LOWER(hostname) = LOWER($1)
	`
	var args []interface{}
	args = append(args, hostName)
	var argCount = 1

	if opts.MinSize > 0 {
		argCount++
		query += fmt.Sprintf(" AND size >= $%d", argCount)
		args = append(args, opts.MinSize)
	}

	query += `
			GROUP BY hash
			HAVING COUNT(*) > 1
		)
		SELECT f.hash, f.path, f.hostname, f.size, h.root_path
		FROM duplicates d
		JOIN files f ON f.hash = d.hash
		JOIN hosts h ON LOWER(h.hostname) = LOWER(f.hostname)
		WHERE LOWER(f.hostname) = LOWER($1)
		ORDER BY d.total_size DESC, d.hash, f.path
	`

	if opts.Count > 0 {
		argCount++
		query += fmt.Sprintf(" LIMIT $%d", argCount)
		args = append(args, opts.Count)
	}

	// Query duplicate groups
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error querying duplicates: %v", err)
	}
	defer rows.Close()

	// Process results
	var currentHash string
	var currentGroup struct {
		Hash      string
		Files     []string
		Hosts     []string
		RootPaths []string
		Size      int64
	}
	var totalMoved, totalSaved int64

	for rows.Next() {
		var hash, path, hostname, rootPath string
		var size int64

		if err := rows.Scan(&hash, &path, &hostname, &size, &rootPath); err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		if hash != currentHash {
			// Process previous group
			if currentHash != "" {
				if err := moveGroupDuplicates(currentGroup, moveOpts, db); err != nil {
					return fmt.Errorf("error moving duplicates for hash %s: %v", currentHash, err)
				}
				totalMoved += int64(len(currentGroup.Files) - 1)
				totalSaved += currentGroup.Size * int64(len(currentGroup.Files)-1)
			}

			// Start new group
			currentHash = hash
			currentGroup = struct {
				Hash      string
				Files     []string
				Hosts     []string
				RootPaths []string
				Size      int64
			}{
				Hash:      hash,
				Size:      size,
				Files:     make([]string, 0),
				Hosts:     make([]string, 0),
				RootPaths: make([]string, 0),
			}
		}

		currentGroup.Files = append(currentGroup.Files, path)
		currentGroup.Hosts = append(currentGroup.Hosts, hostname)
		currentGroup.RootPaths = append(currentGroup.RootPaths, rootPath)
	}

	// Process the last group
	if currentHash != "" {
		if err := moveGroupDuplicates(currentGroup, moveOpts, db); err != nil {
			return fmt.Errorf("error moving duplicates for hash %s: %v", currentHash, err)
		}
		totalMoved += int64(len(currentGroup.Files) - 1)
		totalSaved += currentGroup.Size * int64(len(currentGroup.Files)-1)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	if moveOpts.DryRun {
		fmt.Printf("\nWould move %d files, saving %s bytes\n", totalMoved, formatBytes(totalSaved))
	} else {
		fmt.Printf("\nMoved %d files, saved %s bytes\n", totalMoved, formatBytes(totalSaved))
	}
	return nil
}

// moveGroupDuplicates moves all but one file from a group of duplicates
func moveGroupDuplicates(group struct {
	Hash      string
	Files     []string
	Hosts     []string
	RootPaths []string
	Size      int64
}, opts MoveOptions, db *sql.DB) error {
	if len(group.Files) < 2 {
		return nil // Nothing to move
	}

	// Create a slice to store files with their parent directory counts
	type fileInfo struct {
		path           string
		host           string
		rootPath       string
		parentDirCount int
	}
	files := make([]fileInfo, len(group.Files))

	// Count files in parent directories
	for i, path := range group.Files {
		// Construct full path by joining root path and relative path
		fullPath := filepath.Join(group.RootPaths[i], path)
		parentDir := filepath.Dir(fullPath)
		entries, err := os.ReadDir(parentDir)
		if err != nil {
			// If directory doesn't exist, assign count of 0
			log.Printf("Warning: Could not read directory %s: %v", parentDir, err)
			files[i] = fileInfo{
				path:           path,
				host:           group.Hosts[i],
				rootPath:       group.RootPaths[i],
				parentDirCount: 0,
			}
			continue
		}

		// Count only files (not directories)
		fileCount := 0
		for _, entry := range entries {
			if !entry.IsDir() {
				fileCount++
			}
		}

		files[i] = fileInfo{
			path:           path,
			host:           group.Hosts[i],
			rootPath:       group.RootPaths[i],
			parentDirCount: fileCount,
		}
	}

	// Sort files by parent directory count (ascending)
	// This puts files from least populated directories first
	sort.Slice(files, func(i, j int) bool {
		return files[i].parentDirCount < files[j].parentDirCount
	})

	// Keep the last file (from most populated directory) and move the rest
	fmt.Printf("\nHash: %s (size: %s)\n", group.Hash, formatBytes(group.Size))
	fmt.Printf("Keeping: %s (%s) [parent dir has %d files]\n",
		files[len(files)-1].path,
		files[len(files)-1].host,
		files[len(files)-1].parentDirCount)

	// Move all files except the last one (which is from the most populated directory)
	for i := 0; i < len(files)-1; i++ {
		sourcePath := filepath.Join(files[i].rootPath, files[i].path)

		// Skip if source file doesn't exist
		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			log.Printf("Warning: Source file does not exist: %s", sourcePath)
			continue
		}

		// Create target path
		targetPath := filepath.Join(opts.TargetDir, files[i].path)
		targetDir := filepath.Dir(targetPath)

		if opts.DryRun {
			fmt.Printf("Would move: %s (%s) [parent dir has %d files]\n  -> %s\n",
				sourcePath, files[i].host, files[i].parentDirCount, targetPath)
		} else {
			fmt.Printf("Moving: %s (%s) [parent dir has %d files]\n  -> %s\n",
				sourcePath, files[i].host, files[i].parentDirCount, targetPath)

			// Create target directory
			if err := os.MkdirAll(targetDir, 0755); err != nil {
				return fmt.Errorf("error creating directory %s: %v", targetDir, err)
			}

			// Move the file using rsync to handle cross-filesystem moves
			// First try with os.Rename for efficiency (same filesystem)
			err := os.Rename(sourcePath, targetPath)
			if err != nil {
				// If rename fails due to cross-device link, use rsync
				if strings.Contains(err.Error(), "invalid cross-device link") {
					// Use rsync to copy the file
					cmd := exec.Command("rsync", "-a", "--remove-source-files", sourcePath, targetPath)
					output, err := cmd.CombinedOutput()
					if err != nil {
						return fmt.Errorf("error moving file %s with rsync: %v\nOutput: %s", sourcePath, err, output)
					}
				} else {
					// If it's another error, return it
					return fmt.Errorf("error moving file %s: %v", sourcePath, err)
				}
			}

			// Delete the file from the database
			_, err = db.Exec(`
				DELETE FROM files
				WHERE path = $1 AND host_id = (
					SELECT id FROM hosts WHERE LOWER(hostname) = LOWER($2)
				)
			`, files[i].path, files[i].host)
			if err != nil {
				log.Printf("Warning: Failed to delete file %s from database: %v", files[i].path, err)
			}
		}
	}

	return nil
}
