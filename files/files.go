package files

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
)

func ProcessStdin(db *sql.DB) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error getting hostname: %v", err)
	}
	fmt.Printf("Processing files for host: %s\n", hostname)

	scanner := bufio.NewScanner(os.Stdin)
	var processedCount, errorCount, skippedCount int

	for scanner.Scan() {
		filePath := scanner.Text()

		// Check if path is a directory
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			errorCount++ // Fix syntax error in errorCount increment
			log.Printf("Error accessing file %s: %v", filePath, err)
			continue
		}
		if fileInfo.IsDir() {
			skippedCount++
			fmt.Printf("Skipping directory: %s\n", filePath)
			continue
		}

		fmt.Printf("Processing: %s\n", filePath)

		// Insert or update file in database
		_, err = db.Exec(`
			INSERT INTO files (path, host, size)
			VALUES ($1, $2, $3)
			ON CONFLICT (path, host)
			DO UPDATE SET size = EXCLUDED.size`,
			filePath, hostname, fileInfo.Size())

		if err != nil {
			errorCount++
			log.Printf("Error inserting file %s: %v", filePath, err)
		} else {
			processedCount++
		}
	}

	fmt.Printf("\nSummary:\n")
	fmt.Printf("Host: %s\n", hostname)
	fmt.Printf("Total files processed: %d\n", processedCount)
	fmt.Printf("Directories skipped: %d\n", skippedCount)
	fmt.Printf("Errors encountered: %d\n", errorCount)

	return scanner.Err()
}

func UpdateHashes(db *sql.DB, force bool, count int) error {
	fmt.Println("Updating file hashes...")

	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error getting hostname: %v", err)
	}
	fmt.Printf("Current hostname: %s\n", hostname)

	var query string
	if force {
		query = `SELECT path, host FROM files WHERE host = $1`
		fmt.Println("Force flag enabled: rehashing all files")
	} else {
		query = `SELECT path, host FROM files WHERE hash IS NULL AND host = $1`
		fmt.Println("Processing only files without hashes")
	}

	if count > 0 {
		query += fmt.Sprintf(" LIMIT %d", count)
		fmt.Printf("Processing up to %d files\n", count)
	}

	rows, err := db.Query(query, hostname)
	if err != nil {
		return err
	}
	defer rows.Close()

	var processedCount, errorCount, skippedCount int

	for rows.Next() {
		var path string
		var host string
		if err := rows.Scan(&path, &host); err != nil {
			return err
		}

		// Skip files from other hosts
		if host != hostname {
			fmt.Printf("Skipping file %s (host mismatch: file registered on '%s', current host is '%s')\n", path, host, hostname)
			skippedCount++
			continue
		}

		hash, err := calculateFileHash(path)
		if err != nil {
			errorCount++
			log.Printf("Error calculating hash for %s: %v", path, err)
			continue
		}

		_, err = db.Exec(`
			UPDATE files
			SET hash = $1,
				last_hashed_at = CURRENT_TIMESTAMP
			WHERE path = $2 AND host = $3
		`, hash, path, host)

		if err != nil {
			errorCount++
			log.Printf("Error updating hash for %s: %v", path, err)
		} else {
			processedCount++
			fmt.Printf("Processed %s: %s\n", path, hash)
		}
	}

	fmt.Printf("\nSummary:\n")
	fmt.Printf("Total files processed: %d\n", processedCount)
	fmt.Printf("Files skipped (wrong host): %d\n", skippedCount)
	fmt.Printf("Errors encountered: %d\n", errorCount)

	return rows.Err()
}

// ColorOptions defines ANSI color codes for output formatting
type ColorOptions struct {
	HeaderColor string
	FileColor   string
	ResetColor  string
}

// ListOptions defines the options for listing duplicate files
type ListOptions struct {
	Host     string // Specific host to check for duplicates
	AllHosts bool   // Whether to check across all hosts
	Colors   ColorOptions
}

func FindDuplicates(db *sql.DB, opts ListOptions) error {
	fmt.Println("\nSearching for duplicates...")

	// Get current hostname if no specific host is provided and not checking all hosts
	var hostname string
	var err error
	if !opts.AllHosts && opts.Host == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("error getting hostname: %v", err)
		}
		fmt.Printf("Checking duplicates for current host: %s\n", hostname)
	} else if opts.Host != "" {
		hostname = opts.Host
		fmt.Printf("Checking duplicates for host: %s\n", hostname)
	} else {
		fmt.Println("Checking duplicates across all hosts")
	}

	// Build the query based on options
	query := `
		WITH duplicates AS (
			SELECT hash, size
			FROM files
			WHERE hash IS NOT NULL`

	if !opts.AllHosts {
		query += ` AND host = $1`
	}

	query += `
			GROUP BY hash, size
			HAVING COUNT(*) > 1
		)
		SELECT f.hash, f.size, f.path, f.host, f.last_hashed_at
		FROM files f
		JOIN duplicates d ON f.hash = d.hash
		WHERE f.hash IS NOT NULL`

	if !opts.AllHosts {
		query += ` AND f.host = $1`
	}

	query += ` ORDER BY f.last_hashed_at DESC`

	var rows *sql.Rows
	if !opts.AllHosts {
		rows, err = db.Query(query, hostname)
	} else {
		rows, err = db.Query(query)
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	// Use a map to group files by hash
	type fileInfo struct {
		path         string
		host         string
		lastHashedAt time.Time
	}
	duplicateGroups := make(map[string]struct {
		size  int64
		files []fileInfo
	})

	for rows.Next() {
		var hash string
		var size sql.NullInt64
		var path, host string
		var lastHashedAt time.Time
		if err := rows.Scan(&hash, &size, &path, &host, &lastHashedAt); err != nil {
			return err
		}

		group := duplicateGroups[hash]
		if size.Valid {
			group.size = size.Int64
		}
		group.files = append(group.files, fileInfo{
			path:         path,
			host:         host,
			lastHashedAt: lastHashedAt,
		})
		duplicateGroups[hash] = group
	}

	var totalSpaceSaved int64
	duplicatesFound := len(duplicateGroups)

	// Print results
	for hash, group := range duplicateGroups {
		duplicateCount := len(group.files) - 1
		totalSpaceSaved += group.size * int64(duplicateCount)

		fmt.Printf("\n%sDuplicate files (hash: %s, size: %d bytes):%s\n",
			opts.Colors.HeaderColor, hash, group.size, opts.Colors.ResetColor)

		for _, f := range group.files {
			fmt.Printf("%s  %s [%s] (hashed at: %s)%s\n",
				opts.Colors.FileColor, f.path, f.host,
				f.lastHashedAt.Format("2006-01-02 15:04:05.000000"),
				opts.Colors.ResetColor)
		}
	}

	if duplicatesFound == 0 {
		fmt.Println("No duplicates found")
	} else {
		fmt.Printf("\nFound %d groups of duplicate files\n", duplicatesFound)
		fmt.Printf("Potential disk space savings: %.2f GB\n", float64(totalSpaceSaved)/(1024*1024*1024))
	}

	return rows.Err()
}

func calculateFileHash(filePath string) (string, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("error accessing file: %v", err)
	}
	if fileInfo.IsDir() {
		return "", fmt.Errorf("path is a directory")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	bar := progressbar.NewOptions64(
		fileInfo.Size(),
		progressbar.OptionSetDescription(fmt.Sprintf("Hashing %s...", filepath.Base(filePath))),
		progressbar.OptionSetWidth(15),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionSpinnerType(14),
		// progressbar.OptionSetColor(progressbar.ColorBlue),
	)

	hash := sha256.New()
	if _, err := io.Copy(hash, io.TeeReader(file, bar)); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// PruneOptions defines the options for pruning non-existent files
type PruneOptions struct {
	Host     string // Specific host to prune files from
	AllHosts bool   // Whether to prune files across all hosts
	IAmSure  bool   // Safety flag required for all-hosts pruning
}

// PruneNonExistentFiles removes entries from the database for files that no longer exist
// and generates a report of removed entries.
func PruneNonExistentFiles(db *sql.DB, opts PruneOptions) error {
	// Validate options
	if opts.AllHosts && !opts.IAmSure {
		return fmt.Errorf("pruning across all hosts is a destructive operation that requires additional confirmation")
	}

	// Get current hostname if no specific host is provided and not pruning all hosts
	var hostname string
	var err error
	if !opts.AllHosts && opts.Host == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("error getting hostname: %v", err)
		}
		fmt.Printf("Pruning non-existent files for current host: %s\n", hostname)
	} else if opts.Host != "" {
		hostname = opts.Host
		fmt.Printf("Pruning non-existent files for host: %s\n", hostname)
	} else {
		fmt.Println("Pruning non-existent files across all hosts")
	}

	// Build query based on options
	query := "SELECT id, path, host, size FROM files"
	if !opts.AllHosts {
		query += " WHERE host = $1"
	}

	// Query files
	var rows *sql.Rows
	if !opts.AllHosts {
		rows, err = db.Query(query, hostname)
	} else {
		rows, err = db.Query(query)
	}
	if err != nil {
		return fmt.Errorf("error querying files: %v", err)
	}
	defer rows.Close()

	var removedFiles []string
	var totalSize int64
	removedCount := 0

	// Create report file with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	reportFileName := fmt.Sprintf("pruned_files_report_%s.txt", timestamp)
	reportFile, err := os.Create(reportFileName)
	if err != nil {
		return fmt.Errorf("error creating report file: %v", err)
	}
	defer reportFile.Close()

	// Write report header
	fmt.Fprintf(reportFile, "Pruned Files Report - Generated at %s\n", time.Now().Format(time.RFC3339))
	if !opts.AllHosts {
		fmt.Fprintf(reportFile, "Host: %s\n", hostname)
	} else {
		fmt.Fprintf(reportFile, "Hosts: all\n")
	}
	fmt.Fprintf(reportFile, "\n%-80s %15s %20s\n", "File Path", "Size (bytes)", "Host")
	fmt.Fprintf(reportFile, "%s\n", strings.Repeat("-", 117))

	for rows.Next() {
		var id int
		var path, host string
		var size sql.NullInt64
		if err := rows.Scan(&id, &path, &host, &size); err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		// Check if file exists
		_, err = os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				// File doesn't exist, delete from database
				_, err = db.Exec("DELETE FROM files WHERE id = $1", id)
				if err != nil {
					return fmt.Errorf("error deleting file entry: %v", err)
				}

				// Add to report
				sizeStr := "unknown"
				if size.Valid {
					sizeStr = fmt.Sprintf("%d", size.Int64)
					totalSize += size.Int64 // Only add size for removed files
				}
				fmt.Fprintf(reportFile, "%-80s %15s %20s\n", path, sizeStr, host)
				removedFiles = append(removedFiles, path)
				removedCount++
			} else {
				log.Printf("Warning: Error checking file %s: %v", path, err)
			}
			continue
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	// Write summary to report
	fmt.Fprintf(reportFile, "\nSummary:\n")
	fmt.Fprintf(reportFile, "Total files removed: %d\n", removedCount)
	fmt.Fprintf(reportFile, "Total size of removed files: %d bytes\n", totalSize)

	if !opts.AllHosts {
		fmt.Printf("Pruned %d files from the database for host %s (total size: %d bytes)\n",
			removedCount, hostname, totalSize)
	} else {
		fmt.Printf("Pruned %d files from the database across all hosts (total size: %d bytes)\n",
			removedCount, totalSize)
	}
	fmt.Printf("Report generated: %s\n", reportFileName)

	return nil
}

// OrganizeOptions defines the options for organizing duplicate files
type OrganizeOptions struct {
	Host            string // Specific host to organize files from
	AllHosts        bool   // Whether to organize files across all hosts
	DryRun          bool   // If true, only show what would be done without making changes
	ConflictMoveDir string // If set, move conflicting files to this directory preserving structure
}

func OrganizeDuplicates(db *sql.DB, opts OrganizeOptions) error {
	fmt.Println("\nAnalyzing duplicate files for organization...")

	// Get current hostname if no specific host is provided and not organizing all hosts
	var hostname string
	var err error
	if !opts.AllHosts && opts.Host == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("error getting hostname: %v", err)
		}
		fmt.Printf("Analyzing duplicates for current host: %s\n", hostname)
	} else if opts.Host != "" {
		hostname = opts.Host
		fmt.Printf("Analyzing duplicates for host: %s\n", hostname)
	} else {
		fmt.Println("Analyzing duplicates across all hosts")
	}

	// Build the query based on options
	query := `
		WITH duplicates AS (
			SELECT hash, COUNT(*) as count
			FROM files
			WHERE hash IS NOT NULL`

	if !opts.AllHosts {
		query += ` AND host = $1`
	}

	query += `
			GROUP BY hash
			HAVING COUNT(*) > 1
		)
		SELECT f.hash, f.path, f.host, f.size
		FROM files f
		JOIN duplicates d ON f.hash = d.hash
		WHERE f.hash IS NOT NULL`

	if !opts.AllHosts {
		query += ` AND f.host = $1`
	}

	var rows *sql.Rows
	if !opts.AllHosts {
		rows, err = db.Query(query, hostname)
	} else {
		rows, err = db.Query(query)
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	// Map to store files by hash
	type fileEntry struct {
		path string
		host string
		size int64
	}
	duplicateGroups := make(map[string][]fileEntry)

	// Set to track directories that contain duplicates
	duplicateDirs := make(map[string]bool)

	// Collect all duplicate files and track their directories
	for rows.Next() {
		var hash, path, host string
		var size int64
		if err := rows.Scan(&hash, &path, &host, &size); err != nil {
			return err
		}

		duplicateGroups[hash] = append(duplicateGroups[hash], fileEntry{path, host, size})
		dir := filepath.Dir(path)
		duplicateDirs[dir] = true
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Get total file count, but only for directories containing duplicates
	totalQuery := `
		SELECT path
		FROM files
		WHERE hash IS NOT NULL`
	if !opts.AllHosts {
		totalQuery += ` AND host = $1`
	}

	var totalRows *sql.Rows
	if !opts.AllHosts {
		totalRows, err = db.Query(totalQuery, hostname)
	} else {
		totalRows, err = db.Query(totalQuery)
	}
	if err != nil {
		return err
	}
	defer totalRows.Close()

	// Map to count total files per relevant directory
	dirTotalCount := make(map[string]int)

	for totalRows.Next() {
		var path string
		if err := totalRows.Scan(&path); err != nil {
			return err
		}
		dir := filepath.Dir(path)
		// Only count files in directories that contain duplicates
		if duplicateDirs[dir] {
			dirTotalCount[dir]++
		}
	}

	// Sort directories by total file count
	type dirRank struct {
		path       string
		totalFiles int
	}
	var dirs []dirRank
	for dir := range duplicateDirs {
		dirs = append(dirs, dirRank{
			path:       dir,
			totalFiles: dirTotalCount[dir],
		})
	}
	sort.Slice(dirs, func(i, j int) bool {
		if dirs[i].totalFiles == dirs[j].totalFiles {
			// If total count is equal, prefer shorter paths
			return len(dirs[i].path) < len(dirs[j].path)
		}
		return dirs[i].totalFiles > dirs[j].totalFiles
	})

	// Print directory rankings
	fmt.Println("\nDirectory rankings (only directories containing duplicates):")
	for _, dir := range dirs {
		fmt.Printf("%d total files: %s\n",
			dir.totalFiles, dir.path)
	}

	if len(dirs) == 0 {
		fmt.Println("\nNo duplicate files found to organize.")
		return nil
	}

	// For each group of duplicates, determine which files should be moved
	fmt.Println("\nProposed file moves:")
	var totalMoves int
	var totalBytes int64

	// Track moves for report
	type moveEntry struct {
		SourcePath      string    `json:"source_path"`
		DestinationPath string    `json:"destination_path"`
		FileSize        int64     `json:"file_size"`
		Host            string    `json:"host"`
		MovedAt         time.Time `json:"moved_at"`
	}
	var moves []moveEntry

	for _, files := range duplicateGroups {
		// Only process groups that have actual duplicates
		if len(files) <= 1 {
			continue
		}

		// Find which of these files is in the directory with most files
		var targetFile fileEntry
		targetDir := ""
		for _, file := range files {
			fileDir := filepath.Dir(file.path)
			// Find this directory's rank
			for _, dir := range dirs {
				if dir.path == fileDir {
					// First directory found in the sorted list is the one with most files
					if targetDir == "" {
						targetDir = fileDir
						targetFile = file
					}
					break
				}
			}
		}

		// Move other copies to the target directory
		for _, file := range files {
			currentDir := filepath.Dir(file.path)
			if currentDir == targetDir {
				continue // Skip files already in the target directory
			}

			newPath := filepath.Join(targetDir, filepath.Base(file.path))
			fmt.Printf("Would move:\n  %s\nTo:\n  %s\n", file.path, newPath)
			fmt.Printf("  (This is a duplicate of: %s)\n\n", targetFile.path)
			totalMoves++
			totalBytes += file.size

			if !opts.DryRun {
				// Record planned move for pre-flight check
				moves = append(moves, moveEntry{
					SourcePath:      file.path,
					DestinationPath: newPath,
					FileSize:        file.size,
					Host:            file.host,
					MovedAt:         time.Now(),
				})
			}
		}
	}

	// If we're actually moving files, do a pre-flight check
	if !opts.DryRun && len(moves) > 0 {
		fmt.Println("\nPerforming pre-flight checks...")

		// Check if any destination paths already exist
		var conflicts []string
		var conflictMoves []moveEntry
		for _, move := range moves {
			if _, err := os.Stat(move.DestinationPath); err == nil {
				conflicts = append(conflicts, fmt.Sprintf("  %s (would conflict with move from %s)",
					move.DestinationPath, move.SourcePath))

				if opts.ConflictMoveDir != "" {
					// Calculate new path in conflict directory while preserving structure
					relPath, err := filepath.Rel("/", move.DestinationPath)
					if err != nil {
						return fmt.Errorf("error calculating relative path for %s: %v", move.DestinationPath, err)
					}
					conflictPath := filepath.Join(opts.ConflictMoveDir, relPath)

					conflictMoves = append(conflictMoves, moveEntry{
						SourcePath:      move.DestinationPath, // Move the existing file
						DestinationPath: conflictPath,
						FileSize:        move.FileSize,
						Host:            move.Host,
						MovedAt:         time.Now(),
					})
				}
			} else if !os.IsNotExist(err) {
				// Some other error occurred while checking the file
				return fmt.Errorf("error checking destination path %s: %v", move.DestinationPath, err)
			}
		}

		// If there are conflicts
		if len(conflicts) > 0 {
			if opts.ConflictMoveDir == "" {
				// If no conflict directory specified, abort
				fmt.Println("\nError: Cannot proceed with moves. The following destination paths already exist:")
				for _, conflict := range conflicts {
					fmt.Println(conflict)
				}
				fmt.Println("\nNo files were moved. Please resolve the conflicts by either:")
				fmt.Println("1. Using --move to specify a directory to move conflicting files to")
				fmt.Println("2. Manually moving or removing the conflicting files")
				return nil
			}

			// Only handle conflicts if --move is specified, don't do the actual organization moves
			fmt.Printf("\nFound %d conflicts. Moving existing files to %s...\n", len(conflicts), opts.ConflictMoveDir)

			// First create all required directories
			fmt.Println("Creating directory structure...")
			for _, move := range conflictMoves {
				targetDir := filepath.Dir(move.DestinationPath)
				if err := os.MkdirAll(targetDir, 0755); err != nil {
					return fmt.Errorf("error creating directory structure %s: %v", targetDir, err)
				}
			}

			// Now perform conflict moves
			fmt.Println("Moving files...")
			for _, move := range conflictMoves {
				// Move the conflicting file
				err = os.Rename(move.SourcePath, move.DestinationPath)
				if err != nil {
					return fmt.Errorf("error moving conflicting file %s to %s: %v", move.SourcePath, move.DestinationPath, err)
				}

				// Update the path in the database
				_, err = db.Exec(`
					UPDATE files
					SET path = $1
					WHERE path = $2 AND host = $3`,
					move.DestinationPath, move.SourcePath, move.Host)
				if err != nil {
					// Try to move the file back if database update fails
					if mvErr := os.Rename(move.DestinationPath, move.SourcePath); mvErr != nil {
						return fmt.Errorf("critical error: failed to update database (%v) and failed to move file back (%v)", err, mvErr)
					}
					return fmt.Errorf("error updating file path in database: %v", err)
				}
			}

			// Generate report for conflict moves
			if len(conflictMoves) > 0 {
				// Generate report file with timestamp
				timestamp := time.Now().Format("2006-01-02_15-04-05")
				reportFileName := fmt.Sprintf("conflict_moves_report_%s.json", timestamp)

				// Create report file
				reportFile, err := os.Create(reportFileName)
				if err != nil {
					return fmt.Errorf("error creating report file: %v", err)
				}
				defer reportFile.Close()

				// Create a report structure
				report := struct {
					Timestamp time.Time   `json:"timestamp"`
					Host      string      `json:"host"`
					AllHosts  bool        `json:"all_hosts"`
					Moves     []moveEntry `json:"moves"`
				}{
					Timestamp: time.Now(),
					Host:      hostname,
					AllHosts:  opts.AllHosts,
					Moves:     conflictMoves,
				}

				// Write JSON report
				encoder := json.NewEncoder(reportFile)
				encoder.SetIndent("", "  ")
				if err := encoder.Encode(report); err != nil {
					return fmt.Errorf("error writing report: %v", err)
				}

				fmt.Printf("\nConflict moves report generated: %s\n", reportFileName)
				fmt.Println("Run the organize command again with --run to perform the organization moves.")
			}

			return nil
		}

		fmt.Println("Pre-flight checks passed. Proceeding with moves...")

		// Now perform the actual moves
		for _, move := range moves {
			// Create target directory if it doesn't exist
			targetDir := filepath.Dir(move.DestinationPath)
			err := os.MkdirAll(targetDir, 0755)
			if err != nil {
				return fmt.Errorf("error creating target directory %s: %v", targetDir, err)
			}

			// Move the file
			err = os.Rename(move.SourcePath, move.DestinationPath)
			if err != nil {
				return fmt.Errorf("error moving file %s to %s: %v", move.SourcePath, move.DestinationPath, err)
			}

			// Update the path in the database
			_, err = db.Exec(`
				UPDATE files
				SET path = $1
				WHERE path = $2 AND host = $3`,
				move.DestinationPath, move.SourcePath, move.Host)
			if err != nil {
				// Try to move the file back if database update fails
				if mvErr := os.Rename(move.DestinationPath, move.SourcePath); mvErr != nil {
					return fmt.Errorf("critical error: failed to update database (%v) and failed to move file back (%v)", err, mvErr)
				}
				return fmt.Errorf("error updating file path in database: %v", err)
			}
		}
	}

	fmt.Printf("\nSummary:\n")
	var actionText, sizeText string
	if opts.DryRun {
		actionText = "that would be moved"
		sizeText = "to be moved"
	} else {
		actionText = "moved"
		sizeText = "moved"
	}
	fmt.Printf("Total files %s: %d\n", actionText, totalMoves)
	fmt.Printf("Total size of files %s: %.2f MB\n", sizeText, float64(totalBytes)/(1024*1024))

	if !opts.DryRun && len(moves) > 0 {
		// Generate report file with timestamp
		timestamp := time.Now().Format("2006-01-02_15-04-05")
		reportFileName := fmt.Sprintf("moved_files_report_%s.json", timestamp)

		// Create report file
		reportFile, err := os.Create(reportFileName)
		if err != nil {
			return fmt.Errorf("error creating report file: %v", err)
		}
		defer reportFile.Close()

		// Create a report structure
		report := struct {
			Timestamp time.Time   `json:"timestamp"`
			Host      string      `json:"host"`
			AllHosts  bool        `json:"all_hosts"`
			Moves     []moveEntry `json:"moves"`
		}{
			Timestamp: time.Now(),
			Host:      hostname,
			AllHosts:  opts.AllHosts,
			Moves:     moves,
		}

		// Write JSON report
		encoder := json.NewEncoder(reportFile)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return fmt.Errorf("error writing report: %v", err)
		}

		fmt.Printf("\nMove report generated: %s\n", reportFileName)
		fmt.Println("This report can be used to undo the moves if needed.")
	} else if opts.DryRun {
		fmt.Println("This was a dry run. No files were actually moved.")
		fmt.Println("Use --run to actually move the files.")
	} else {
		fmt.Println("All files have been moved successfully.")
	}

	return nil
}
