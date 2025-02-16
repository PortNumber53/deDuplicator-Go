package files

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/schollz/progressbar/v3"
)

// calculateFileHash computes the SHA-256 hash of a file
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

	// Create a progress bar for this file
	bar := progressbar.NewOptions64(fileInfo.Size(),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(30),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan]Hashing %s", filepath.Base(filePath))),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	hash := sha256.New()
	reader := bufio.NewReader(file)
	buf := make([]byte, 1024*1024) // 1MB buffer

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			hash.Write(buf[:n])
			bar.Add64(int64(n))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	fmt.Println() // Add newline after progress bar
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ProcessStdin processes file paths from stdin
func ProcessStdin(ctx context.Context, db *sql.DB) error {
	// Get hostname for current machine
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error getting hostname: %v", err)
	}

	// Find host in database by hostname
	var hostName string
	err = db.QueryRow(`
		SELECT name 
		FROM hosts 
		WHERE hostname = $1
	`, hostname).Scan(&hostName)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add'", hostname)
		}
		return fmt.Errorf("error finding host: %v", err)
	}

	// Begin transaction for batch inserts
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Prepare statement for batch inserts
	stmt, err := tx.Prepare(`
		INSERT INTO files (path, hostname, size)
		VALUES ($1, $2, $3)
		ON CONFLICT (path, hostname) 
		DO UPDATE SET size = EXCLUDED.size
	`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	// Process each line from stdin
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled")
		default:
		}

		path := scanner.Text()

		// Get file info
		info, err := os.Stat(path)
		if err != nil {
			log.Printf("Warning: Error accessing file %s: %v", path, err)
			continue
		}

		// Skip directories
		if info.IsDir() {
			continue
		}

		// Insert file into database
		_, err = stmt.Exec(path, hostName, info.Size())
		if err != nil {
			log.Printf("Warning: Error inserting file %s: %v", path, err)
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading stdin: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

// UpdateHashes updates file hashes in the database
func UpdateHashes(db *sql.DB, force bool, count int) error {
	// Get hostname for current machine
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error getting hostname: %v", err)
	}

	// Find host in database by hostname
	var hostName string
	err = db.QueryRow(`
		SELECT name 
		FROM hosts 
		WHERE hostname = $1
	`, hostname).Scan(&hostName)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add'", hostname)
		}
		return fmt.Errorf("error finding host: %v", err)
	}

	// Build query based on options
	query := `
		SELECT id, path 
		FROM files 
		WHERE hostname = $1 AND hash IS NULL
	`
	if force {
		query = `
			SELECT id, path 
			FROM files 
			WHERE hostname = $1
		`
	}
	if count > 0 {
		query += fmt.Sprintf(" LIMIT %d", count)
	}

	// First, count total files to process
	var totalFiles int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS subquery", query)
	err = db.QueryRow(countQuery, hostName).Scan(&totalFiles)
	if err != nil {
		return fmt.Errorf("error counting files: %v", err)
	}

	if totalFiles == 0 {
		fmt.Println("No files need hashing")
		return nil
	}

	// Create progress bar
	bar := progressbar.NewOptions64(totalFiles,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan]Processing files..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Prepare update statement
	stmt, err := db.Prepare(`
		UPDATE files 
		SET hash = $1, last_hashed_at = NOW()
		WHERE id = $2
	`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	// Query files to process
	rows, err := db.Query(query, hostName)
	if err != nil {
		return fmt.Errorf("error querying files: %v", err)
	}
	defer rows.Close()

	// Process each file
	for rows.Next() {
		var id int
		var path string
		err := rows.Scan(&id, &path)
		if err != nil {
			log.Printf("Warning: Error scanning row: %v", err)
			continue
		}

		// Calculate hash
		hash, err := calculateFileHash(path)
		if err != nil {
			log.Printf("Warning: Error hashing file %s: %v", path, err)
			continue
		}

		// Update database
		_, err = stmt.Exec(hash, id)
		if err != nil {
			log.Printf("Warning: Error updating hash for file %s: %v", path, err)
			continue
		}

		bar.Add(1)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	fmt.Printf("\nSuccessfully processed %d files\n", totalFiles)
	return nil
}

// ColorOptions represents color settings for output
type ColorOptions struct {
	HeaderColor string
	FileColor   string
	ResetColor  string
}

// DuplicateListOptions represents options for listing duplicate files
type DuplicateListOptions struct {
	Host     string // Specific host to check for duplicates
	AllHosts bool   // Whether to check across all hosts
	Count    int    // Limit the number of duplicate groups to show (0 = no limit)
	MinSize  int64  // Minimum file size to consider
	Colors   ColorOptions
}

// FindDuplicates finds and displays duplicate files
func FindDuplicates(ctx context.Context, db *sql.DB, opts DuplicateListOptions) error {
	// If no host specified and not all hosts, use current hostname
	if opts.Host == "" && !opts.AllHosts {
		// Get hostname for current machine
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("error getting hostname: %v", err)
		}

		// Convert hostname to lowercase for consistency
		hostname = strings.ToLower(hostname)
		log.Printf("Looking up host for hostname: %s", hostname)

		// Find host in database by hostname (case-insensitive)
		err = db.QueryRow(`
			SELECT hostname
			FROM hosts
			WHERE LOWER(hostname) = LOWER($1)
		`, hostname).Scan(&opts.Host)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add' or specify --host", hostname)
			}
			return fmt.Errorf("error finding host: %v", err)
		}
		log.Printf("Found host: %s", opts.Host)
	}

	// Build query based on options
	query := `
		WITH duplicates AS (
			SELECT hash, COUNT(*) as count, SUM(size) as total_size
			FROM files
			WHERE hash IS NOT NULL
	`
	var args []interface{}
	var argCount int

	if opts.Host != "" {
		argCount++
		query += fmt.Sprintf(" AND hostname = $%d", argCount)
		args = append(args, opts.Host)
	}

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
	`

	if opts.Host != "" {
		argCount++
		query += fmt.Sprintf(" AND f.hostname = $%d", argCount)
		args = append(args, opts.Host)
	}

	query += ` ORDER BY d.total_size DESC, d.hash, f.path`

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
		// Skip if not all hosts when AllHosts is true
		if opts.AllHosts {
			allHosts := make(map[string]bool)
			for _, host := range group.Hosts {
				allHosts[host] = true
			}
			if len(allHosts) < 2 {
				continue
			}
		}

		fmt.Printf("%sHash: %s%s\n", opts.Colors.HeaderColor, group.Hash, opts.Colors.ResetColor)
		fmt.Printf("Size: %s bytes\n", formatBytes(group.Size))
		fmt.Printf("Duplicates: %d files\n", len(group.Files))
		fmt.Println("Files:")
		for i, file := range group.Files {
			fmt.Printf("%s  %s (%s)%s\n",
				opts.Colors.FileColor,
				file,
				group.Hosts[i],
				opts.Colors.ResetColor)
		}
		savings := group.Size * int64(len(group.Files)-1)
		fmt.Printf("Potential savings: %s bytes\n", formatBytes(savings))
		totalSavings += savings
		fmt.Println()
	}

	fmt.Printf("\nTotal potential space savings: %s bytes\n", formatBytes(totalSavings))
	return nil
}

// formatBytes formats a byte count with thousand separators
func formatBytes(bytes int64) string {
	// Convert to string first
	str := fmt.Sprintf("%d", bytes)

	// Add thousand separators
	var result []byte
	for i := len(str) - 1; i >= 0; i-- {
		if i != len(str)-1 && (len(str)-i-1)%3 == 0 {
			result = append([]byte{','}, result...)
		}
		result = append([]byte{str[i]}, result...)
	}

	return string(result)
}

// PruneOptions represents options for the prune command
type PruneOptions struct {
	Host     string // Specific host to prune files from
	AllHosts bool   // Whether to prune files across all hosts
	IAmSure  bool   // Safety flag required for all-hosts pruning
}

// PruneNonExistentFiles removes entries for files that no longer exist
func PruneNonExistentFiles(ctx context.Context, db *sql.DB, opts PruneOptions) error {
	if opts.AllHosts && !opts.IAmSure {
		return fmt.Errorf("refusing to prune all hosts without --i-am-sure flag")
	}

	// Get list of hosts to process
	var hosts []struct {
		name     string
		rootPath string
	}

	query := "SELECT name, root_path FROM hosts"
	if opts.Host != "" {
		query += fmt.Sprintf(" WHERE name = '%s'", opts.Host)
	}

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying hosts: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var host struct {
			name     string
			rootPath string
		}
		err := rows.Scan(&host.name, &host.rootPath)
		if err != nil {
			return fmt.Errorf("error scanning host: %v", err)
		}
		hosts = append(hosts, host)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating hosts: %v", err)
	}

	// Process each host
	for _, host := range hosts {
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled while processing host %s", host.name)
		default:
		}

		fmt.Printf("Checking files for host '%s'...\n", host.name)

		// Get files for this host
		query := `
			SELECT id, path 
			FROM files 
			WHERE hostname = $1
		`
		rows, err := db.Query(query, host.name)
		if err != nil {
			return fmt.Errorf("error querying files: %v", err)
		}

		// Begin transaction for batch deletes
		tx, err := db.Begin()
		if err != nil {
			rows.Close()
			return fmt.Errorf("error starting transaction: %v", err)
		}

		// Prepare delete statement
		stmt, err := tx.Prepare(`DELETE FROM files WHERE id = $1`)
		if err != nil {
			rows.Close()
			tx.Rollback()
			return fmt.Errorf("error preparing statement: %v", err)
		}

		// Check each file
		var deleted int
		for rows.Next() {
			var id int
			var path string
			err := rows.Scan(&id, &path)
			if err != nil {
				log.Printf("Warning: Error scanning row: %v", err)
				continue
			}

			// Check if file exists
			fullPath := filepath.Join(host.rootPath, path)
			_, err = os.Stat(fullPath)
			if os.IsNotExist(err) {
				// Delete from database
				_, err = stmt.Exec(id)
				if err != nil {
					log.Printf("Warning: Error deleting file %s: %v", path, err)
					continue
				}
				deleted++
			}
		}

		rows.Close()

		// Commit transaction
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("error committing transaction: %v", err)
		}

		fmt.Printf("Removed %d entries for non-existent files\n", deleted)
	}

	return nil
}

// OrganizeOptions represents options for the organize command
type OrganizeOptions struct {
	Host            string // Specific host to organize files from
	AllHosts        bool   // Whether to organize files across all hosts
	DryRun          bool   // If true, only show what would be done without making changes
	ConflictMoveDir string // If set, move conflicting files to this directory preserving structure
	StripPrefix     string // Remove this prefix from paths when moving files
}

// OrganizeDuplicates organizes duplicate files
func OrganizeDuplicates(ctx context.Context, db *sql.DB, opts OrganizeOptions) error {
	// Build query based on options
	query := `
		SELECT hash, array_agg(path) as paths, array_agg(hostname) as hosts, array_agg(size) as sizes
		FROM files 
		WHERE hash IS NOT NULL
	`
	if opts.Host != "" {
		query += fmt.Sprintf(" AND hostname = '%s'", opts.Host)
	}
	query += `
		GROUP BY hash 
		HAVING COUNT(*) > 1
		ORDER BY array_length(array_agg(path), 1) DESC
	`

	// Query duplicate groups
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying duplicates: %v", err)
	}
	defer rows.Close()

	// Process each duplicate group
	var totalGroups, totalFiles int
	for rows.Next() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled after processing %d duplicate groups", totalGroups)
		default:
		}

		var hash string
		var paths, hosts []string
		var sizes []int64
		err := rows.Scan(&hash, &paths, &hosts, &sizes)
		if err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		// Skip if not all hosts when AllHosts is true
		if opts.AllHosts {
			allHosts := make(map[string]bool)
			for _, host := range hosts {
				allHosts[host] = true
			}
			if len(allHosts) < 2 {
				continue
			}
		}

		// Get root paths for each host
		hostRoots := make(map[string]string)
		for _, host := range hosts {
			if _, ok := hostRoots[host]; !ok {
				var rootPath string
				err := db.QueryRow(`
					SELECT root_path 
					FROM hosts 
					WHERE name = $1
				`, host).Scan(&rootPath)
				if err != nil {
					return fmt.Errorf("error getting root path for host %s: %v", host, err)
				}
				hostRoots[host] = rootPath
			}
		}

		// Print duplicate group
		fmt.Printf("Hash: %s\n", hash)
		for i := range paths {
			fmt.Printf("  %s (%s, %d bytes)\n", paths[i], hosts[i], sizes[i])
		}
		fmt.Println()

		totalGroups++
		totalFiles += len(paths)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	if totalGroups == 0 {
		fmt.Println("No duplicates found")
	} else {
		fmt.Printf("Found %d duplicate groups with %d total files\n", totalGroups, totalFiles)
	}

	return nil
}

// DedupeOptions represents options for the dedupe command
type DedupeOptions struct {
	DryRun        bool   // If true, only show what would be done without making changes
	DestDir       string // Directory to move duplicate files to
	StripPrefix   string // Remove this prefix from paths when moving files
	Count         int    // Limit the number of duplicate groups to process (0 = no limit)
	IgnoreDestDir bool   // If true, ignore files that are already in the destination directory
}

// DedupFiles deduplicates files by moving them to a destination directory
func DedupFiles(ctx context.Context, db *sql.DB, opts DedupeOptions) error {
	// Ensure destination directory exists
	if err := os.MkdirAll(opts.DestDir, 0755); err != nil {
		return fmt.Errorf("error creating destination directory: %v", err)
	}

	// Get hostname for current machine
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error getting hostname: %v", err)
	}

	// Find host in database by hostname
	var hostName string
	err = db.QueryRow(`
		SELECT name 
		FROM hosts 
		WHERE hostname = $1
	`, hostname).Scan(&hostName)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add'", hostname)
		}
		return fmt.Errorf("error finding host: %v", err)
	}

	// Build query to find duplicates
	query := `
		SELECT hash, array_agg(path) as paths, array_agg(hostname) as hosts, array_agg(size) as sizes
		FROM files 
		WHERE hash IS NOT NULL AND hostname = $1
		GROUP BY hash 
		HAVING COUNT(*) > 1
		ORDER BY array_length(array_agg(path), 1) DESC
	`
	if opts.Count > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Count)
	}

	// Query duplicate groups
	rows, err := db.Query(query, hostName)
	if err != nil {
		return fmt.Errorf("error querying duplicates: %v", err)
	}
	defer rows.Close()

	// Get root path for current host
	var rootPath string
	err = db.QueryRow(`
		SELECT root_path 
		FROM hosts 
		WHERE name = $1
	`, hostName).Scan(&rootPath)
	if err != nil {
		return fmt.Errorf("error getting root path: %v", err)
	}

	// Process each duplicate group
	var totalGroups, totalFiles int
	for rows.Next() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled after processing %d duplicate groups", totalGroups)
		default:
		}

		var hash string
		var paths, hosts []string
		var sizes []int64
		err := rows.Scan(&hash, &paths, &hosts, &sizes)
		if err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		// Skip if any file is in destination directory
		if opts.IgnoreDestDir {
			inDest := false
			for _, path := range paths {
				if strings.HasPrefix(path, opts.DestDir) {
					inDest = true
					break
				}
			}
			if inDest {
				continue
			}
		}

		// Print duplicate group
		fmt.Printf("Hash: %s\n", hash)
		for i := range paths {
			fmt.Printf("  %s (%s, %d bytes)\n", paths[i], hosts[i], sizes[i])
		}
		fmt.Println()

		totalGroups++
		totalFiles += len(paths)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	if totalGroups == 0 {
		fmt.Println("No duplicates found")
	} else {
		fmt.Printf("Found %d duplicate groups with %d total files\n", totalGroups, totalFiles)
	}

	return nil
}

// calculateDestPath calculates the destination path for a file
func calculateDestPath(sourcePath, destDir, stripPrefix string) (string, error) {
	// Remove prefix if specified
	if stripPrefix != "" {
		if strings.HasPrefix(sourcePath, stripPrefix) {
			sourcePath = sourcePath[len(stripPrefix):]
		}
	}

	// Clean up path
	sourcePath = filepath.Clean(sourcePath)
	if strings.HasPrefix(sourcePath, "/") {
		sourcePath = sourcePath[1:]
	}

	// Join with destination directory
	destPath := filepath.Join(destDir, sourcePath)

	// Create parent directory
	parentDir := filepath.Dir(destPath)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return "", fmt.Errorf("error creating directory %s: %v", parentDir, err)
	}

	return destPath, nil
}

// ParseSize parses a human-readable size string (e.g., "1.5G", "500M", "10K") into bytes
func ParseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	if sizeStr == "" {
		return 0, nil
	}

	// If it's just a number, treat as bytes
	if num, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
		return num, nil
	}

	// Extract the numeric part and unit
	var numStr string
	var unit string
	for i, c := range sizeStr {
		if c >= '0' && c <= '9' || c == '.' {
			numStr += string(c)
		} else {
			unit = strings.ToUpper(sizeStr[i:])
			break
		}
	}

	if numStr == "" {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	// Parse the numeric part
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number in size: %s", sizeStr)
	}

	// Convert to bytes based on unit
	var multiplier float64
	switch unit {
	case "K", "KB":
		multiplier = 1024
	case "M", "MB":
		multiplier = 1024 * 1024
	case "G", "GB":
		multiplier = 1024 * 1024 * 1024
	case "T", "TB":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "B", "":
		multiplier = 1
	default:
		return 0, fmt.Errorf("unknown size unit: %s", unit)
	}

	return int64(num * multiplier), nil
}

// MoveOptions represents options for moving duplicate files
type MoveOptions struct {
	TargetDir string // Directory to move duplicates to
	DryRun    bool   // If true, only show what would be done
}

// MoveDuplicates moves duplicate files to a target directory
func MoveDuplicates(ctx context.Context, db *sql.DB, opts DuplicateListOptions, moveOpts MoveOptions) error {
	// Create target directory if it doesn't exist
	if !moveOpts.DryRun {
		if err := os.MkdirAll(moveOpts.TargetDir, 0755); err != nil {
			return fmt.Errorf("error creating target directory: %v", err)
		}
	}

	// Build query based on options
	query := `
		WITH duplicates AS (
			SELECT hash, COUNT(*) as count, SUM(size) as total_size
			FROM files
			WHERE hash IS NOT NULL
	`
	var args []interface{}
	var argCount int

	if opts.Host != "" {
		argCount++
		query += fmt.Sprintf(" AND LOWER(hostname) = LOWER($%d)", argCount)
		args = append(args, opts.Host)
	}

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
	`

	if opts.Host != "" {
		argCount++
		query += fmt.Sprintf(" AND LOWER(f.hostname) = LOWER($%d)", argCount)
		args = append(args, opts.Host)
	}

	query += ` ORDER BY d.total_size DESC, d.hash, f.path`

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
				if err := moveGroupDuplicates(currentGroup, moveOpts); err != nil {
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
		if err := moveGroupDuplicates(currentGroup, moveOpts); err != nil {
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
}, opts MoveOptions) error {
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

			// Move the file
			if err := os.Rename(sourcePath, targetPath); err != nil {
				return fmt.Errorf("error moving file %s: %v", sourcePath, err)
			}
		}
	}

	return nil
}
