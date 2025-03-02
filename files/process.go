package files

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

// ProcessStdin processes a list of files from standard input and adds them to the database
func ProcessStdin(ctx context.Context, db *sql.DB) error {
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
		SELECT name
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

	// Begin transaction
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

	// Read file paths from stdin
	scanner := bufio.NewScanner(os.Stdin)
	var processed, skipped int

	for scanner.Scan() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled after processing %d files", processed)
		default:
		}

		path := scanner.Text()
		if path == "" {
			continue
		}

		// Get file info
		fileInfo, err := os.Lstat(path)
		if err != nil {
			log.Printf("Warning: Error accessing path %s: %v", path, err)
			skipped++
			continue
		}

		// Skip directories
		if fileInfo.IsDir() {
			log.Printf("Skipping directory: %s", path)
			skipped++
			continue
		}

		// Skip symlinks, device files, etc.
		if !fileInfo.Mode().IsRegular() {
			log.Printf("Skipping non-regular file: %s", path)
			skipped++
			continue
		}

		// Insert file into database
		_, err = stmt.Exec(path, hostName, fileInfo.Size())
		if err != nil {
			log.Printf("Warning: Error inserting file %s: %v", path, err)
			skipped++
			continue
		}

		processed++
		if processed%100 == 0 {
			log.Printf("Processed %d files so far...", processed)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading from stdin: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	log.Printf("Successfully processed %d files, skipped %d files", processed, skipped)
	return nil
}

// ProcessFiles processes files in the given directory and adds them to the database
func ProcessFiles(ctx context.Context, db *sql.DB, dir string, opts FindOptions) error {
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
		SELECT name
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

	// Get host information
	var host struct {
		id       int
		name     string
		rootPath string
	}

	err = db.QueryRow("SELECT id, name, root_path FROM hosts WHERE name = $1", hostName).Scan(&host.id, &host.name, &host.rootPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("host not found: %s", hostName)
		}
		return fmt.Errorf("error querying host: %v", err)
	}

	// Ensure directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("directory does not exist: %s", dir)
	}

	// Ensure directory is within host root path
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("error getting absolute path: %v", err)
	}

	absRootPath, err := filepath.Abs(host.rootPath)
	if err != nil {
		return fmt.Errorf("error getting absolute root path: %v", err)
	}

	if !strings.HasPrefix(absDir, absRootPath) {
		return fmt.Errorf("directory %s is not within host root path %s", absDir, absRootPath)
	}

	// Count files to process
	fmt.Printf("Counting files in %s...\n", dir)
	var totalFiles int
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Warning: Error accessing path %s: %v", path, err)
			return nil
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Skip symlinks, device files, etc.
		if !info.Mode().IsRegular() {
			return nil
		}

		// Skip files smaller than minimum size
		if opts.MinimumSize > 0 && info.Size() < opts.MinimumSize {
			return nil
		}

		totalFiles++
		return nil
	})
	if err != nil {
		return fmt.Errorf("error counting files: %v", err)
	}

	fmt.Printf("Found %d files to process\n", totalFiles)
	if totalFiles == 0 {
		fmt.Println("No files to process")
		return nil
	}

	// Create progress bar
	bar := progressbar.NewOptions64(int64(totalFiles),
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

	// Default to 4 workers if not specified
	numWorkers := 4
	if opts.NumWorkers > 0 {
		numWorkers = opts.NumWorkers
	}

	// Create worker pool
	var wg sync.WaitGroup
	fileChan := make(chan string, numWorkers*2)
	resultChan := make(chan struct {
		path     string
		hash     string
		size     int64
		modTime  time.Time
		err      error
		duration time.Duration
	}, numWorkers*2)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range fileChan {
				start := time.Now()
				hash, err := calculateFileHash(path)
				duration := time.Since(start)

				if err != nil {
					resultChan <- struct {
						path     string
						hash     string
						size     int64
						modTime  time.Time
						err      error
						duration time.Duration
					}{path: path, err: err, duration: duration}
					continue
				}

				// Get file info
				info, err := os.Stat(path)
				if err != nil {
					resultChan <- struct {
						path     string
						hash     string
						size     int64
						modTime  time.Time
						err      error
						duration time.Duration
					}{path: path, err: err, duration: duration}
					continue
				}

				resultChan <- struct {
					path     string
					hash     string
					size     int64
					modTime  time.Time
					err      error
					duration time.Duration
				}{path: path, hash: hash, size: info.Size(), modTime: info.ModTime(), err: nil, duration: duration}
			}
		}()
	}

	// Start result processor
	var processed, errors, skipped, added, updated int
	var totalBytes int64
	var totalDuration time.Duration
	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()

		// Begin transaction
		tx, err := db.Begin()
		if err != nil {
			log.Printf("Error starting transaction: %v", err)
			return
		}
		defer tx.Rollback()

		// Prepare statements
		insertStmt, err := tx.Prepare(`
			INSERT INTO files (hash, path, size, mod_time, hostname)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (hash, path, hostname) DO UPDATE
			SET size = $3, mod_time = $4
		`)
		if err != nil {
			log.Printf("Error preparing insert statement: %v", err)
			return
		}
		defer insertStmt.Close()

		// Check if file exists in database
		checkStmt, err := tx.Prepare(`
			SELECT hash, size, mod_time
			FROM files
			WHERE path = $1 AND LOWER(hostname) = LOWER($2)
		`)
		if err != nil {
			log.Printf("Error preparing check statement: %v", err)
			return
		}
		defer checkStmt.Close()

		for result := range resultChan {
			processed++
			if result.err != nil {
				log.Printf("Error processing file %s: %v", result.path, result.err)
				errors++
				bar.Add(1)
				continue
			}

			// Get relative path from root
			relPath, err := filepath.Rel(host.rootPath, result.path)
			if err != nil {
				log.Printf("Error getting relative path for %s: %v", result.path, result.err)
				errors++
				bar.Add(1)
				continue
			}

			// Check if file exists in database with same hash and mod time
			var dbHash string
			var dbSize int64
			var dbModTime time.Time
			err = checkStmt.QueryRow(relPath, host.name).Scan(&dbHash, &dbSize, &dbModTime)
			if err == nil {
				// File exists in database
				if dbHash == result.hash && dbSize == result.size && dbModTime.Equal(result.modTime) {
					// File hasn't changed, skip
					skipped++
					bar.Add(1)
					continue
				}
				// File has changed, update
				updated++
			} else if err != sql.ErrNoRows {
				// Error checking file
				log.Printf("Error checking file %s: %v", relPath, err)
				errors++
				bar.Add(1)
				continue
			} else {
				// File doesn't exist in database, add it
				added++
			}

			// Insert or update file in database
			_, err = insertStmt.Exec(result.hash, relPath, result.size, result.modTime, host.name)
			if err != nil {
				log.Printf("Error inserting file %s: %v", relPath, err)
				errors++
				bar.Add(1)
				continue
			}

			totalBytes += result.size
			totalDuration += result.duration
			bar.Add(1)
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			log.Printf("Error committing transaction: %v", err)
			return
		}
	}()

	// Walk directory and send files to workers
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Warning: Error accessing path %s: %v", path, err)
			return nil
		}

		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Skip symlinks, device files, etc.
		if !info.Mode().IsRegular() {
			return nil
		}

		// Skip files smaller than minimum size
		if opts.MinimumSize > 0 && info.Size() < opts.MinimumSize {
			return nil
		}

		// Send file to worker
		fileChan <- path
		return nil
	})
	if err != nil {
		return fmt.Errorf("error walking directory: %v", err)
	}

	// Close file channel and wait for workers to finish
	close(fileChan)
	wg.Wait()
	close(resultChan)
	resultWg.Wait()

	fmt.Printf("\nProcessed %d files (%s)\n", processed, formatBytes(totalBytes))
	fmt.Printf("Added %d files, updated %d files, skipped %d files, errors %d\n", added, updated, skipped, errors)

	if totalDuration > 0 && totalBytes > 0 {
		bytesPerSecond := float64(totalBytes) / totalDuration.Seconds()
		fmt.Printf("Average processing speed: %s/s\n", formatBytes(int64(bytesPerSecond)))
	}

	return nil
}
