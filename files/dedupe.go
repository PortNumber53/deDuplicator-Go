package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
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
	if !opts.DryRun {
		if err := os.MkdirAll(opts.DestDir, 0755); err != nil {
			return fmt.Errorf("error creating destination directory: %v", err)
		}
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

		// Process the group for deduplication if not in dry run mode
		if !opts.DryRun {
			if err := deduplicateGroup(group, rootPath, opts); err != nil {
				return fmt.Errorf("error deduplicating group with hash %s: %v", group.Hash, err)
			}
		}

		totalGroups++
		totalFiles += len(group.Files)
	}

	if opts.DryRun {
		fmt.Printf("\nTotal potential space savings: %s bytes\n", formatBytes(totalSavings))
		fmt.Println("Dry run mode - no files were moved. Use --run to actually move files.")
	} else {
		fmt.Printf("\nTotal space saved: %s bytes\n", formatBytes(totalSavings))
	}

	return nil
}

// deduplicateGroup handles the deduplication of a single group of duplicate files
func deduplicateGroup(group DuplicateGroup, rootPath string, opts DedupeOptions) error {
	if len(group.Files) < 2 {
		return nil // Nothing to deduplicate
	}

	// Create a slice to store files with their parent directory counts
	type fileInfo struct {
		path           string
		host           string
		parentDirCount int
	}
	files := make([]fileInfo, len(group.Files))

	// Count files in parent directories
	for i, path := range group.Files {
		// Construct full path by joining root path and relative path
		fullPath := filepath.Join(rootPath, path)
		parentDir := filepath.Dir(fullPath)
		entries, err := os.ReadDir(parentDir)
		if err != nil {
			// If directory doesn't exist, assign count of 0
			log.Printf("Warning: Could not read directory %s: %v", parentDir, err)
			files[i] = fileInfo{
				path:           path,
				host:           group.Hosts[i],
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
		sourcePath := filepath.Join(rootPath, files[i].path)

		// Skip if source file doesn't exist
		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			log.Printf("Warning: Source file does not exist: %s", sourcePath)
			continue
		}

		// Create target path, applying strip prefix if specified
		targetPath := files[i].path
		if opts.StripPrefix != "" && strings.HasPrefix(targetPath, opts.StripPrefix) {
			targetPath = targetPath[len(opts.StripPrefix):]
			// Remove leading slash if present
			targetPath = strings.TrimPrefix(targetPath, "/")
		}
		targetPath = filepath.Join(opts.DestDir, targetPath)
		targetDir := filepath.Dir(targetPath)

		fmt.Printf("Moving: %s (%s) [parent dir has %d files]\n  -> %s\n",
			sourcePath, files[i].host, files[i].parentDirCount, targetPath)

		// Create target directory
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return fmt.Errorf("error creating directory %s: %v", targetDir, err)
		}

		// Move the file
		if err := os.Rename(sourcePath, targetPath); err != nil {
			return fmt.Errorf("error moving file %s: %v", sourcePath, err)
		}
	}

	return nil
}
