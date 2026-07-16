package files

import (
	"context"
	"database/sql"
	"deduplicator/logging"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type duplicateMoveGroup struct {
	Hash      string
	Files     []string
	Hosts     []string
	RootPaths []string
	Size      int64
}

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
	logging.InfoLogger.Printf("Looking up host for hostname: %s", hostname)

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
	logging.InfoLogger.Printf("Found host: %s", hostName)

	// Build query based on options
	query := `
		WITH duplicate_hashes AS (
			SELECT hash, size, SUM(size) as total_size
			FROM files
			WHERE hash IS NOT NULL
			AND hash NOT IN ('TIMEOUT_ERROR', 'HASH_ERROR')
			AND size IS NOT NULL
	`
	var args []interface{}
	var argCount int

	if opts.MinSize > 0 {
		argCount++
		query += fmt.Sprintf(" AND size >= $%d", argCount)
		args = append(args, opts.MinSize)
	}

	query += `
			GROUP BY hash, size
			HAVING COUNT(*) > 1
			ORDER BY total_size DESC, hash, size
	`
	if opts.Count > 0 {
		argCount++
		query += fmt.Sprintf(" LIMIT $%d", argCount)
		args = append(args, opts.Count)
	}
	query += `
		)
		SELECT f.hash, f.path, f.hostname, f.size, COALESCE(f.root_folder, '') as root_folder
		FROM duplicate_hashes d
		JOIN files f ON f.hash = d.hash AND f.size = d.size
		ORDER BY d.total_size DESC, d.hash, d.size, f.hostname, f.path
	`

	// Query duplicate groups
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error querying duplicates: %v", err)
	}
	defer rows.Close()

	// Process results
	var currentHash string
	var currentSize int64
	var currentGroup duplicateMoveGroup
	var totalMoved, totalSaved int64

	for rows.Next() {
		var hash, path, hostname, rootPath string
		var size int64

		if err := rows.Scan(&hash, &path, &hostname, &size, &rootPath); err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		if hash != currentHash || size != currentSize {
			// Process previous group
			if currentHash != "" {
				moved, err := moveGroupDuplicates(currentGroup, moveOpts, db, hostName)
				if err != nil {
					return fmt.Errorf("error moving duplicates for hash %s: %v", currentHash, err)
				}
				totalMoved += moved
				totalSaved += currentGroup.Size * moved
			}

			// Start new group
			currentHash = hash
			currentSize = size
			currentGroup = duplicateMoveGroup{
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
		// Debug log for root paths
		logging.InfoLogger.Printf("[DEBUG] Looping through these root paths: %v", currentGroup.RootPaths)
		moved, err := moveGroupDuplicates(currentGroup, moveOpts, db, hostName)
		if err != nil {
			return fmt.Errorf("error moving duplicates for hash %s: %v", currentHash, err)
		}
		totalMoved += moved
		totalSaved += currentGroup.Size * moved
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

// moveGroupDuplicates moves local duplicate files that are not the deterministic global keeper.
func moveGroupDuplicates(group duplicateMoveGroup, opts MoveOptions, db *sql.DB, localHost string) (int64, error) {
	if len(group.Files) < 2 {
		return 0, nil // Nothing to move
	}

	// Create a slice to store files with their parent directory counts
	type fileInfo struct {
		path           string
		host           string
		rootPath       string
		sourcePath     string
		local          bool
		parentDirCount int
	}
	files := make([]fileInfo, len(group.Files))

	// Count local files in parent directories. Remote hosts are never inspected or moved
	// by this process; each host archives its own files when the command runs there.
	for i, path := range group.Files {
		info := fileInfo{
			path:     path,
			host:     group.Hosts[i],
			rootPath: group.RootPaths[i],
			local:    strings.EqualFold(group.Hosts[i], localHost),
		}
		if filepath.IsAbs(path) {
			info.sourcePath = path
		} else {
			info.sourcePath = filepath.Join(group.RootPaths[i], path)
		}

		if !info.local {
			files[i] = info
			continue
		}

		logging.InfoLogger.Printf("[DEBUG] Using rootPath: %s, path: %s, fullPath: %s", group.RootPaths[i], path, info.sourcePath)
		parentDir := filepath.Dir(info.sourcePath)
		entries, err := os.ReadDir(parentDir)
		if err != nil {
			// If directory doesn't exist, assign count of 0
			logging.ErrorLogger.Printf("Warning: Could not read directory %s: %v", parentDir, err)
			files[i] = info
			continue
		}

		// Count only files (not directories)
		fileCount := 0
		for _, entry := range entries {
			if !entry.IsDir() {
				fileCount++
			}
		}

		info.parentDirCount = fileCount
		files[i] = info
	}

	// Use only database attributes for the global keeper so every host reaches
	// the same decision even though each process can only inspect its local disk.
	sort.Slice(files, func(i, j int) bool {
		if files[i].host != files[j].host {
			return files[i].host < files[j].host
		}
		if files[i].rootPath != files[j].rootPath {
			return files[i].rootPath < files[j].rootPath
		}
		return files[i].path < files[j].path
	})

	keeper := files[0]
	hasLocalMove := false
	for i := 1; i < len(files); i++ {
		if files[i].local {
			hasLocalMove = true
			break
		}
	}
	if !hasLocalMove {
		return 0, nil
	}

	fmt.Printf("\nHash: %s (size: %s)\n", group.Hash, formatBytes(group.Size))
	fmt.Printf("Keeping: %s (%s)\n", keeper.path, keeper.host)

	var moved int64
	for i := 1; i < len(files); i++ {
		if !files[i].local {
			continue
		}
		sourcePath := files[i].sourcePath
		logging.InfoLogger.Printf("[DEBUG] Moving file. rootPath: %s, path: %s, sourcePath: %s", files[i].rootPath, files[i].path, sourcePath)

		// Skip if source file doesn't exist
		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			logging.ErrorLogger.Printf("Warning: Source file does not exist: %s", sourcePath)
			continue
		}

		// Create target path
		targetPath := filepath.Join(opts.TargetDir, files[i].host, archiveRelativePath(files[i].path))
		targetDir := filepath.Dir(targetPath)

		if opts.DryRun {
			fmt.Printf("Would move: %s (%s) [parent dir has %d files]\n  -> %s\n",
				sourcePath, files[i].host, files[i].parentDirCount, targetPath)
		} else {
			fmt.Printf("Moving: %s (%s) [parent dir has %d files]\n  -> %s\n",
				sourcePath, files[i].host, files[i].parentDirCount, targetPath)

			// Create target directory
			if err := os.MkdirAll(targetDir, 0755); err != nil {
				return moved, fmt.Errorf("error creating directory %s: %v", targetDir, err)
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
						return moved, fmt.Errorf("error moving file %s with rsync: %v\nOutput: %s", sourcePath, err, output)
					}
				} else {
					// If it's another error, return it
					return moved, fmt.Errorf("error moving file %s: %v", sourcePath, err)
				}
			}

			// Delete the file from the database
			_, err = db.Exec(`
				DELETE FROM files
				WHERE path = $1
				AND LOWER(hostname) = LOWER($2)
				AND COALESCE(root_folder, '') = $3
			`, files[i].path, files[i].host, files[i].rootPath)
			if err != nil {
				logging.ErrorLogger.Printf("Warning: Failed to delete file %s from database: %v", files[i].path, err)
			}
		}
		moved++
	}

	return moved, nil
}

func archiveRelativePath(path string) string {
	cleaned := filepath.Clean(path)
	if volume := filepath.VolumeName(cleaned); volume != "" {
		cleaned = strings.TrimPrefix(cleaned, volume)
	}
	cleaned = strings.TrimLeft(cleaned, string(filepath.Separator))
	for cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		cleaned = strings.TrimPrefix(cleaned, "..")
		cleaned = strings.TrimLeft(cleaned, string(filepath.Separator))
	}
	if cleaned == "." || cleaned == "" {
		return filepath.Base(path)
	}
	return cleaned
}
