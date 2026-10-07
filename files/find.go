package files

import (
	"context"
	"database/sql"
	"deduplicator/db"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"log"
	"os"
	"path/filepath"
)

// FindFiles indexes regular files, committing complete batches of 1000.
// Cancellation rolls back the unfinished batch.
func FindFiles(ctx context.Context, sqldb *sql.DB, opts FindOptions) (resultErr error) {
	defer cancellationResult(ctx, &resultErr)
	if err := ctx.Err(); err != nil {
		return err
	}
	host, err := db.GetHostContext(ctx, sqldb, opts.Server)
	if err != nil {
		return err
	}
	paths, err := host.GetPaths()
	if err != nil {
		return fmt.Errorf("error decoding host paths: %w", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no paths configured for server: %s", opts.Server)
	}
	if opts.Path != "" {
		root, ok := paths[opts.Path]
		if !ok {
			return fmt.Errorf("friendly path '%s' not found for server '%s'", opts.Path, opts.Server)
		}
		paths = map[string]string{opts.Path: root}
	}
	var tx *sql.Tx
	var stmt *sql.Stmt
	defer func() {
		if stmt != nil {
			stmt.Close()
		}
		if tx != nil {
			tx.Rollback()
		}
	}()
	startBatch := func() error {
		var err error
		tx, err = sqldb.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		stmt, err = tx.PrepareContext(ctx, `INSERT INTO files (path, hostname, size, root_folder)
   VALUES ($1, $2, $3, $4) ON CONFLICT (path, hostname)
   DO UPDATE SET size = EXCLUDED.size, root_folder = EXCLUDED.root_folder`)
		return err
	}
	if err := startBatch(); err != nil {
		return err
	}
	var processed, batch int64
	bar := newProgressBar(-1, "Finding files...", progressbar.OptionShowCount(), progressbar.OptionSetWidth(15), progressbar.OptionSpinnerType(14))
	for friendly, root := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		log.Printf("Scanning path '%s': %s", friendly, root)
		if _, err := os.Stat(root); os.IsNotExist(err) {
			log.Printf("Warning: path does not exist: %s", root)
			continue
		}
		err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if walkErr != nil {
				log.Printf("Warning: Error accessing path %s: %v", path, walkErr)
				return nil
			}
			if info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				log.Printf("Warning: Error getting relative path for %s: %v", path, err)
				return nil
			}
			if _, err = stmt.ExecContext(ctx, rel, host.Hostname, info.Size(), root); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("Warning: Error inserting file %s: %v", rel, err)
				return nil
			}
			processed++
			batch++
			bar.Add(1)
			if batch >= 1000 {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := tx.Commit(); err != nil {
					return err
				}
				stmt.Close()
				batch = 0
				return startBatch()
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("error walking directory: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if batch > 0 {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	fmt.Printf("\nSuccessfully processed %d files for \"%s\"\n", processed, host.Name)
	return nil
}
