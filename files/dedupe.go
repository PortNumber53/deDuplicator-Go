package files

import (
	"context"
	"database/sql"
	"fmt"
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

	// Find duplicate groups
	groups, err := FindDuplicateGroups(ctx, db, hostname, opts.MinSize, opts.Count)
	if err != nil {
		return err
	}

	// Get root path for current host
	var rootPath string
	err = db.QueryRow(`
		SELECT root_path 
		FROM hosts 
		WHERE LOWER(name) = LOWER($1)
	`, hostname).Scan(&rootPath)
	if err != nil {
		return fmt.Errorf("error getting root path: %v", err)
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
