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
func ProcessStdin(ctx context.Context, db *sql.DB) (resultErr error) {
	defer cancellationResult(ctx, &resultErr)
	if err := ctx.Err(); err != nil {
		return err
	}
	// Get hostname for current machine
	hostname, err := os.Hostname()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("error getting hostname: %v", err)
	}

	// Convert hostname to lowercase for consistency
	hostname = strings.ToLower(hostname)
	log.Printf("Looking up host for hostname: %s", hostname)

	// Find host in database by hostname (case-insensitive)
	var hostName string
	err = db.QueryRowContext(ctx, `
		SELECT name
		FROM hosts
		WHERE LOWER(hostname) = LOWER($1)
	`, hostname).Scan(&hostName)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == sql.ErrNoRows {
			return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add'", hostname)
		}
		return fmt.Errorf("error finding host: %v", err)
	}
	log.Printf("Found host: %s", hostName)

	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Prepare statement for batch inserts
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO files (path, hostname, size)
		VALUES ($1, $2, $3)
		ON CONFLICT (path, hostname) 
		DO UPDATE SET size = EXCLUDED.size
	`)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	// Read file paths from stdin
	input := os.Stdin
	stopWatch := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			input.Close()
		case <-stopWatch:
		}
	}()
	defer func() { close(stopWatch); <-watchDone }()
	scanner := bufio.NewScanner(input)
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
			if ctx.Err() != nil {
				return ctx.Err()
			}
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
		_, err = stmt.ExecContext(ctx, path, hostName, fileInfo.Size())
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("Warning: Error inserting file %s: %v", path, err)
			skipped++
			continue
		}

		processed++
		if processed%100 == 0 {
			log.Printf("Processed %d files so far...", processed)
		}
	}

	if err := ctx.Err(); err != nil {
		return err
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
func ProcessFiles(ctx context.Context, db *sql.DB, dir string, opts FindOptions) (resultErr error) {
	defer cancellationResult(ctx, &resultErr)
	if err := ctx.Err(); err != nil {
		return err
	}
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
	err = db.QueryRowContext(ctx, `
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

	err = db.QueryRowContext(ctx, "SELECT id, name, root_path FROM hosts WHERE name = $1", hostName).Scan(&host.id, &host.name, &host.rootPath)
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
		if ctx.Err() != nil {
			return ctx.Err()
		}
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
	bar := newProgressBar(int64(totalFiles), "Processing files...",
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15))

	// Default to 4 workers if not specified
	numWorkers := 4
	if opts.NumWorkers > 0 {
		numWorkers = opts.NumWorkers
	}

	// Establish the transaction before launching producers, so setup failure
	// cannot leave workers blocked on a result channel with no consumer.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	insertStmt, err := tx.PrepareContext(ctx, `INSERT INTO files (hash, path, size, mod_time, hostname)
 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (hash, path, hostname) DO UPDATE SET size = $3, mod_time = $4`)
	if err != nil {
		return err
	}
	defer insertStmt.Close()
	checkStmt, err := tx.PrepareContext(ctx, `SELECT hash, size, mod_time FROM files WHERE path = $1 AND LOWER(hostname) = LOWER($2)`)
	if err != nil {
		return err
	}
	defer checkStmt.Close()
	workCtx, cancel := context.WithCancel(ctx)
	type fileResult struct {
		path, hash string
		size       int64
		modTime    time.Time
		err        error
		duration   time.Duration
	}
	fileChan := make(chan string, numWorkers*2)
	resultChan := make(chan fileResult, numWorkers*2)
	walkResult := make(chan error, 1)
	walkDone := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if workCtx.Err() != nil {
					return
				}
				var path string
				select {
				case <-workCtx.Done():
					return
				case next, ok := <-fileChan:
					if !ok {
						return
					}
					path = next
				}
				if workCtx.Err() != nil {
					return
				}
				started := time.Now()
				hash, err := calculateFileHashContext(workCtx, path)
				result := fileResult{path: path, hash: hash, err: err, duration: time.Since(started)}
				if err == nil {
					info, err := os.Stat(path)
					result.err = err
					if err == nil {
						result.size = info.Size()
						result.modTime = info.ModTime()
					}
				}
				select {
				case <-workCtx.Done():
					return
				case resultChan <- result:
				}
			}
		}()
	}
	go func() {
		defer close(walkDone)
		defer close(fileChan)
		walkResult <- filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
			if err := workCtx.Err(); err != nil {
				return err
			}
			if walkErr != nil {
				log.Printf("Warning: Error accessing path %s: %v", path, walkErr)
				return nil
			}
			if !info.Mode().IsRegular() || (opts.MinimumSize > 0 && info.Size() < opts.MinimumSize) {
				return nil
			}
			select {
			case <-workCtx.Done():
				return workCtx.Err()
			case fileChan <- path:
				return nil
			}
		})
	}()
	workersDone := make(chan struct{})
	go func() { wg.Wait(); close(resultChan); close(workersDone) }()
	defer func() { cancel(); <-walkDone; <-workersDone }()
	var processed, failed, skipped, added, updated int
	var totalBytes int64
	var totalDuration time.Duration
	for result := range resultChan {
		if err := ctx.Err(); err != nil {
			return err
		}
		processed++
		if result.err != nil {
			log.Printf("Error processing file %s: %v", result.path, result.err)
			failed++
			bar.Add(1)
			continue
		}
		rel, err := filepath.Rel(host.rootPath, result.path)
		if err != nil {
			log.Printf("Error getting relative path for %s: %v", result.path, err)
			failed++
			continue
		}
		var oldHash string
		var oldSize int64
		var oldTime time.Time
		err = checkStmt.QueryRowContext(ctx, rel, host.name).Scan(&oldHash, &oldSize, &oldTime)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			if oldHash == result.hash && oldSize == result.size && oldTime.Equal(result.modTime) {
				skipped++
				bar.Add(1)
				continue
			}
			updated++
		} else if err != sql.ErrNoRows {
			log.Printf("Error checking file %s: %v", rel, err)
			failed++
			continue
		} else {
			added++
		}
		_, err = insertStmt.ExecContext(ctx, result.hash, rel, result.size, result.modTime, host.name)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			log.Printf("Error inserting file %s: %v", rel, err)
			failed++
			continue
		}
		totalBytes += result.size
		totalDuration += result.duration
		bar.Add(1)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := <-walkResult; err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Printf("\nProcessed %d files (%s)\n", processed, formatBytes(totalBytes))
	fmt.Printf("Added %d files, updated %d files, skipped %d files, errors %d\n", added, updated, skipped, failed)
	if totalDuration > 0 && totalBytes > 0 {
		fmt.Printf("Average processing speed: %s/s\n", formatBytes(int64(float64(totalBytes)/totalDuration.Seconds())))
	}
	return nil
}
