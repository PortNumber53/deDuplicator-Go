package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
)

// FindDuplicates finds and displays duplicate files
func FindDuplicates(ctx context.Context, db *sql.DB, opts DuplicateListOptions) error {
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
	`

	// If count is specified, limit the number of duplicate groups
	if opts.Count > 0 {
		argCount++
		query += fmt.Sprintf(" ORDER BY total_size DESC LIMIT $%d", argCount)
		args = append(args, opts.Count)
	} else {
		query += ` ORDER BY total_size DESC`
	}

	query += `
		)
		SELECT f.hash, f.path, f.hostname, f.size
		FROM duplicates d
		JOIN files f ON f.hash = d.hash
		WHERE LOWER(f.hostname) = LOWER($1)
		ORDER BY d.total_size DESC, d.hash, f.path
	`

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

	// Print results
	if len(groups) == 0 {
		fmt.Println("No duplicate files found.")
		return nil
	}

	var totalSavings int64
	fmt.Printf("Found %d groups of duplicate files:\n\n", len(groups))
	for _, group := range groups {
		// Print duplicate group with colors (matching dedupe command format)
		fmt.Printf("\033[33mHash: %s\033[0m\n", group.Hash)
		fmt.Printf("Size: %s bytes\n", formatBytes(group.Size))
		fmt.Printf("Duplicates: %d files\n", len(group.Files))
		fmt.Println("Files:")
		for i, file := range group.Files {
			fmt.Printf("\033[90m  %s (%s)\033[0m\n",
				file,
				group.Hosts[i])
		}
		savings := group.Size * int64(len(group.Files)-1)
		fmt.Printf("Potential savings: %s bytes\n", formatBytes(savings))
		totalSavings += savings
		fmt.Println()
	}

	fmt.Printf("\nTotal potential space savings: %s bytes\n", formatBytes(totalSavings))
	return nil
}
