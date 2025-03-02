package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DuplicateGroup represents a group of duplicate files
type DuplicateGroup struct {
	Hash      string
	Size      int64
	Files     []string
	Hosts     []string
	TotalSize int64
}

// FindDuplicateGroups finds groups of duplicate files based on the provided options
func FindDuplicateGroups(ctx context.Context, db *sql.DB, hostname string, minSize int64, count int) ([]DuplicateGroup, error) {
	// Find host in database by hostname (case-insensitive)
	var hostName string
	err := db.QueryRow(`
		SELECT hostname
		FROM hosts
		WHERE LOWER(hostname) = LOWER($1)
	`, hostname).Scan(&hostName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add'", hostname)
		}
		return nil, fmt.Errorf("error finding host: %v", err)
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

	if minSize > 0 {
		argCount++
		query += fmt.Sprintf(" AND size >= $%d", argCount)
		args = append(args, minSize)
	}

	query += `
			GROUP BY hash
			HAVING COUNT(*) > 1
	`

	// If count is specified, limit the number of duplicate groups
	if count > 0 {
		argCount++
		query += fmt.Sprintf(" ORDER BY total_size DESC LIMIT $%d", argCount)
		args = append(args, count)
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
		return nil, fmt.Errorf("error querying duplicates: %v", err)
	}
	defer rows.Close()

	// Process results
	var currentHash string
	var currentGroup DuplicateGroup
	var groups []DuplicateGroup

	for rows.Next() {
		var hash, path, hostname string
		var size int64

		if err := rows.Scan(&hash, &path, &hostname, &size); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		if hash != currentHash {
			if currentHash != "" {
				groups = append(groups, currentGroup)
			}
			currentHash = hash
			currentGroup = DuplicateGroup{
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
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return groups, nil
}

// PrintDuplicateGroups prints the duplicate groups in a formatted way
func PrintDuplicateGroups(groups []DuplicateGroup) int64 {
	if len(groups) == 0 {
		fmt.Println("No duplicate files found.")
		return 0
	}

	var totalSavings int64
	fmt.Printf("Found %d groups of duplicate files:\n\n", len(groups))
	for _, group := range groups {
		// Print duplicate group with colors
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
	return totalSavings
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
