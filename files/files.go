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
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan]Hashing %s...", filepath.Base(filePath))),
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
		INSERT INTO files (path, host, size)
		VALUES ($1, $2, $3)
		ON CONFLICT (path, host) 
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
		WHERE host = $1 AND hash IS NULL
	`
	if force {
		query = `
			SELECT id, path 
			FROM files 
			WHERE host = $1
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

// ListOptions represents options for the list command
type ListOptions struct {
	Host     string // Specific host to check for duplicates
	AllHosts bool   // Whether to check across all hosts
	Count    int    // Limit the number of duplicate groups to show (0 = no limit)
	Colors   ColorOptions
}

// FindDuplicates finds and displays duplicate files
func FindDuplicates(ctx context.Context, db *sql.DB, opts ListOptions) error {
	// Build query based on options
	query := `
		SELECT hash, array_agg(path) as paths, array_agg(host) as hosts, array_agg(size) as sizes
		FROM files 
		WHERE hash IS NOT NULL
	`
	if opts.Host != "" {
		query += fmt.Sprintf(" AND host = '%s'", opts.Host)
	}
	query += `
		GROUP BY hash 
		HAVING COUNT(*) > 1
		ORDER BY array_length(array_agg(path), 1) DESC
	`
	if opts.Count > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Count)
	}

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

		// Print duplicate group
		fmt.Printf("%sHash: %s%s\n", opts.Colors.HeaderColor, hash, opts.Colors.ResetColor)
		for i := range paths {
			fmt.Printf("%s%s (%s, %d bytes)%s\n",
				opts.Colors.FileColor,
				paths[i],
				hosts[i],
				sizes[i],
				opts.Colors.ResetColor)
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
			WHERE host = $1
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
		SELECT hash, array_agg(path) as paths, array_agg(host) as hosts, array_agg(size) as sizes
		FROM files 
		WHERE hash IS NOT NULL
	`
	if opts.Host != "" {
		query += fmt.Sprintf(" AND host = '%s'", opts.Host)
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
		SELECT hash, array_agg(path) as paths, array_agg(host) as hosts, array_agg(size) as sizes
		FROM files 
		WHERE hash IS NOT NULL AND host = $1
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
