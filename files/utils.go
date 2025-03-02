package files

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

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
