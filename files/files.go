package files

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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
		fileInfo, err := os.Stat(path)
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
					totalSize += size.Int64
				}
				fmt.Fprintf(reportFile, "%-80s %15s %20s\n", path, sizeStr, host)
				removedFiles = append(removedFiles, path)
				removedCount++
			} else {
				log.Printf("Warning: Error checking file %s: %v", path, err)
			}
			continue
		}

		// If we get here, the file exists
		if size.Valid {
			totalSize += fileInfo.Size()
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
