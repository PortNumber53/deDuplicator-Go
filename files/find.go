package files

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"deduplicator/db"

	"github.com/schollz/progressbar/v3"
)

// FindFiles traverses the root path of the specified host and adds files to the database
func FindFiles(ctx context.Context, sqldb *sql.DB, opts FindOptions) error {
	// Get host and its paths
	host, err := db.GetHost(sqldb, opts.Server)

	paths, err := host.GetPaths()
	if err != nil {
		return fmt.Errorf("error decoding host paths: %v", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no paths configured for server: %s", opts.Server)
	}
	log.Printf("Found %d paths for server '%s'", len(paths), host.Name)

	var processedFiles int64
	var currentBatch int64
	var tx *sql.Tx
	var stmt *sql.Stmt

	// Function to start a new transaction
	startNewTransaction := func() error {
		// If we have an existing transaction, commit it
		if tx != nil {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("error committing transaction: %v", err)
			}
			stmt.Close()
		}

		// Start new transaction
		tx, err = sqldb.Begin()
		if err != nil {
			return fmt.Errorf("error starting transaction: %v", err)
		}

		// Prepare statement for batch inserts
		stmt, err = tx.Prepare(`
			INSERT INTO files (path, hostname, size, root_folder)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (path, hostname)
			DO UPDATE SET size = EXCLUDED.size, root_folder = EXCLUDED.root_folder
		`)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error preparing statement: %v", err)
		}

		currentBatch = 0
		return nil
	}

	// Start initial transaction
	if err := startNewTransaction(); err != nil {
		return err
	}

	// Create progress bar (indeterminate)
	bar := progressbar.NewOptions(-1,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan]Finding files..."),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Walk all configured paths, or just the requested one if opts.Path is set
	if opts.Path != "" {
		rootPath, ok := paths[opts.Path]
		if !ok {
			return fmt.Errorf("friendly path '%s' not found for server '%s'", opts.Path, host.Name)
		}
		log.Printf("Scanning path '%s': %s", opts.Path, rootPath)
		if _, err := os.Stat(rootPath); os.IsNotExist(err) {
			log.Printf("Warning: path does not exist: %s", rootPath)
			return nil
		}
		err = filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err != nil {
				log.Printf("Warning: Error accessing path %s: %v", path, err)
				return nil
			}
			if info.IsDir() || (info.Mode()&os.ModeSymlink) != 0 {
				return nil
			}
			relPath, err := filepath.Rel(rootPath, path)
			if err != nil {
				log.Printf("Warning: Error getting relative path for %s: %v", path, err)
				return nil
			}
			dbPath := relPath
			_, err = stmt.Exec(dbPath, host.Hostname, info.Size(), rootPath)
			if err != nil {
				log.Printf("Warning: Error inserting file %s: %v", dbPath, err)
				return nil
			}
			processedFiles++
			currentBatch++
			if currentBatch >= 1000 {
				if err := startNewTransaction(); err != nil {
					return err
				}
				bar.Describe(fmt.Sprintf("[cyan]Finding files... (%d processed)[reset]", processedFiles))
			}
			bar.Add(1)
			return nil
		})
		if err != nil {
			if err == context.Canceled {
				if tx != nil {
					if err := tx.Commit(); err != nil {
						log.Printf("Warning: Error committing final batch: %v", err)
					} else {
						log.Printf("Successfully committed final batch")
					}
				}
				fmt.Printf("\nOperation cancelled after processing %d files\n", processedFiles)
				return fmt.Errorf("operation cancelled")
			}
			return fmt.Errorf("error walking directory: %v", err)
		}
		log.Printf("\n%s Done processing \"%s\"", time.Now().Format("2006/01/02 15:04:05"), opts.Path)
	} else {
		for friendly, rootPath := range paths {
			log.Printf("Scanning path '%s': %s", friendly, rootPath)
			if _, err := os.Stat(rootPath); os.IsNotExist(err) {
				log.Printf("Warning: path does not exist: %s", rootPath)
				continue
			}
			select {
			case <-ctx.Done():
				fmt.Printf("\nOperation cancelled after processing %d files\n", processedFiles)
				return fmt.Errorf("operation cancelled")
			default:
			}
			err = filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if err != nil {
					log.Printf("Warning: Error accessing path %s: %v", path, err)
					return nil
				}
				if info.IsDir() || (info.Mode()&os.ModeSymlink) != 0 {
					return nil
				}
				relPath, err := filepath.Rel(rootPath, path)
				if err != nil {
					log.Printf("Warning: Error getting relative path for %s: %v", path, err)
					return nil
				}
				dbPath := relPath
				_, err = stmt.Exec(dbPath, host.Hostname, info.Size(), rootPath)
				if err != nil {
					log.Printf("Warning: Error inserting file %s: %v", dbPath, err)
					return nil
				}
				processedFiles++
				currentBatch++
				if currentBatch >= 1000 {
					if err := startNewTransaction(); err != nil {
						return err
					}
					bar.Describe(fmt.Sprintf("[cyan]Finding files... (%d processed)[reset]", processedFiles))
				}
				bar.Add(1)
				return nil
			})
			if err != nil {
				if err == context.Canceled {
					if tx != nil {
						if err := tx.Commit(); err != nil {
							log.Printf("Warning: Error committing final batch: %v", err)
						} else {
							log.Printf("Successfully committed final batch")
						}
					}
					fmt.Printf("\nOperation cancelled after processing %d files\n", processedFiles)
					return fmt.Errorf("operation cancelled")
				}
				return fmt.Errorf("error walking directory: %v", err)
			}
			log.Printf("\n%s Done processing \"%s\"", time.Now().Format("2006/01/02 15:04:05"), friendly)
		}
	}


	if err != nil {
		if err == context.Canceled {
			// Try to commit the last batch before returning
			if tx != nil {
				if err := tx.Commit(); err != nil {
					log.Printf("Warning: Error committing final batch: %v", err)
				} else {
					log.Printf("Successfully committed final batch")
				}
			}
			fmt.Printf("\nOperation cancelled after processing %d files\n", processedFiles)
			return fmt.Errorf("operation cancelled")
		}
		return fmt.Errorf("error walking directory: %v", err)
	}

	// Commit final transaction if there are any remaining files
	if currentBatch > 0 && tx != nil {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("error committing final transaction: %v", err)
		}
	}

	fmt.Printf("\nSuccessfully processed %d files for \"%s\"\n", processedFiles, host.Name)
	return nil
}
