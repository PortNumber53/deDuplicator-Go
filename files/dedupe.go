package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// DedupFiles deduplicates files by moving them to a destination directory
func DedupFiles(ctx context.Context, db *sql.DB, opts DedupeOptions) error {
	// Check if the destination directory is valid
	if opts.DestDir == "" {
		return fmt.Errorf("destination directory cannot be empty")
	}

	// Check if the parent directory exists
	parentDir := filepath.Dir(opts.DestDir)
	if _, err := os.Stat(parentDir); os.IsNotExist(err) {
		return fmt.Errorf("parent directory %s does not exist, please create it first", parentDir)
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(opts.DestDir, 0755); err != nil {
		return fmt.Errorf("error creating destination directory: %v", err)
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

	// Build query based on options - using the same structure as FindDuplicates
	query := `
		WITH duplicates AS (
			SELECT hash, COUNT(*) as count, SUM(size) as total_size
			FROM files
			WHERE hash IS NOT NULL AND LOWER(hostname) = LOWER($1)
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
		SELECT f.hash, f.path, f.hostname, f.size
		FROM duplicates d
		JOIN files f ON f.hash = d.hash
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

	// Get root path for current host
	var rootPath string
	err = db.QueryRow(`
		SELECT root_path 
		FROM hosts 
		WHERE LOWER(name) = LOWER($1)
	`, hostName).Scan(&rootPath)
	if err != nil {
		return fmt.Errorf("error getting root path: %v", err)
	}

	// Process results
	var currentHash string
	var currentGroup struct {
		Hash      string
		Size      int64
		Files     []string
		Hosts     []string
		TotalSize int64
	}
	var groups []struct {
		Hash      string
		Size      int64
		Files     []string
		Hosts     []string
		TotalSize int64
	}

	for rows.Next() {
		var hash, path, hostname string
		var size int64

		if err := rows.Scan(&hash, &path, &hostname, &size); err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		if hash != currentHash {
			if currentHash != "" {
				groups = append(groups, currentGroup)
			}
			currentHash = hash
			currentGroup = struct {
				Hash      string
				Size      int64
				Files     []string
				Hosts     []string
				TotalSize int64
			}{
				Hash:  hash,
				Size:  size,
				Files: make([]string, 0),
				Hosts: make([]string, 0),
			}
		}
		currentGroup.Files = append(currentGroup.Files, path)
		currentGroup.Hosts = append(currentGroup.Hosts, hostname)
		currentGroup.TotalSize += size
	}

	// Add the last group
	if currentHash != "" {
		groups = append(groups, currentGroup)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	// Process duplicate groups
	var totalGroups, totalFiles int
	var totalSavings int64

	if len(groups) == 0 {
		fmt.Println("No duplicates found")
		return nil
	}

	fmt.Printf("Found %d groups of duplicate files:\n\n", len(groups))
	for _, group := range groups {
		// Skip if any file is in destination directory
		if opts.IgnoreDestDir {
			inDest := false
			for _, path := range group.Files {
				if strings.HasPrefix(path, opts.DestDir) {
					inDest = true
					break
				}
			}
			if inDest {
				continue
			}
		}

		// Print duplicate group with colors
		fmt.Printf("\033[33mHash: %s\033[0m\n", group.Hash)
		fmt.Printf("Size: %s bytes\n", formatBytes(group.Size))
		fmt.Printf("Duplicates: %d files\n", len(group.Files))
		fmt.Println("Files:")
		for i := range group.Files {
			fmt.Printf("\033[90m  %s (%s)\033[0m\n",
				group.Files[i],
				group.Hosts[i])
		}
		savings := group.Size * int64(len(group.Files)-1)
		fmt.Printf("Potential savings: %s bytes\n", formatBytes(savings))
		totalSavings += savings
		fmt.Println()

		totalGroups++
		totalFiles += len(group.Files)
	}

	fmt.Printf("\nTotal potential space savings: %s bytes\n", formatBytes(totalSavings))
	return nil
}
