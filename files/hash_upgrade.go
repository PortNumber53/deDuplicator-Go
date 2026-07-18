package files

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"deduplicator/db"
	"deduplicator/logging"
)

const hashUpgradeBatchSize = 100

// UpgradeStoredHashes recalculates full-file hashes for files with existing stored hashes.
func UpgradeStoredHashes(ctx context.Context, sqldb *sql.DB, opts HashUpgradeOptions) error {
	host, err := db.GetHostByHostname(sqldb, opts.Server)
	if err != nil {
		host, err = db.GetHost(sqldb, opts.Server)
		if err != nil {
			return fmt.Errorf("server not found: %s", opts.Server)
		}
	}
	hostname := host.Hostname

	whereClause := `
			WHERE LOWER(hostname) = LOWER($1)
			AND hash IS NOT NULL
			AND hash NOT IN ('TIMEOUT_ERROR', 'HASH_ERROR')
		`

	var total int64
	if err := sqldb.QueryRowContext(ctx, "SELECT COUNT(*) FROM files "+whereClause, hostname).Scan(&total); err != nil {
		return fmt.Errorf("error counting stored hashes: %v", err)
	}
	if total == 0 {
		fmt.Println("No stored hashes found for upgrade.")
		return nil
	}

	query := fmt.Sprintf(`
		SELECT id, path, root_folder, hash
		FROM files
		%s
		AND id > $2
		ORDER BY id ASC
		LIMIT %d
	`, whereClause, hashUpgradeBatchSize)

	lastID := 0
	var checked, upgraded, unchanged, failed int64
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled after checking %d of %d stored hashes", checked, total)
		default:
		}

		rows, err := sqldb.QueryContext(ctx, query, hostname, lastID)
		if err != nil {
			return fmt.Errorf("error querying stored hashes: %v", err)
		}

		batchCount := 0
		for rows.Next() {
			var id int
			var dbPath string
			var rootFolder sql.NullString
			var storedHash string
			if err := rows.Scan(&id, &dbPath, &rootFolder, &storedHash); err != nil {
				rows.Close()
				return fmt.Errorf("error scanning stored hash row: %v", err)
			}

			lastID = id
			batchCount++
			checked++

			fullPath := dbPath
			if rootFolder.Valid && strings.TrimSpace(rootFolder.String) != "" {
				fullPath = filepath.Join(rootFolder.String, dbPath)
			}

			fullHash, err := calculateFileHash(fullPath)
			if err != nil {
				failed++
				logging.ErrorLogger.Printf("Warning: Error recalculating full hash for %s: %v", fullPath, err)
				continue
			}

			if fullHash == storedHash {
				unchanged++
				continue
			}

			if _, err := sqldb.ExecContext(ctx, `
				UPDATE files
				SET hash = $1, last_hashed_at = NOW()
				WHERE id = $2
			`, fullHash, id); err != nil {
				failed++
				logging.ErrorLogger.Printf("Warning: Error updating full hash for %s: %v", fullPath, err)
				continue
			}
			upgraded++
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating stored hashes: %v", err)
		}
		if batchCount < hashUpgradeBatchSize {
			break
		}
	}

	fmt.Printf("Hash upgrade completed: checked %d stored hashes, upgraded %d, unchanged %d, failed %d\n", checked, upgraded, unchanged, failed)
	return nil
}
