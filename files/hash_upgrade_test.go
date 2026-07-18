package files

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestUpgradeStoredHashesUpdatesOnlyChangedHashes(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()
	mock.MatchExpectationsInOrder(false)

	root := t.TempDir()
	changedContent := []byte(strings.Repeat("a", 2048) + "changed suffix")
	sameContent := []byte("already full")
	if err := os.WriteFile(filepath.Join(root, "changed.bin"), changedContent, 0644); err != nil {
		t.Fatalf("write changed fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "same.bin"), sameContent, 0644); err != nil {
		t.Fatalf("write same fixture: %v", err)
	}

	partialHashBytes := sha256.Sum256(changedContent[:1024])
	partialHash := hex.EncodeToString(partialHashBytes[:])
	changedFullHashBytes := sha256.Sum256(changedContent)
	changedFullHash := hex.EncodeToString(changedFullHashBytes[:])
	sameFullHashBytes := sha256.Sum256(sameContent)
	sameFullHash := hex.EncodeToString(sameFullHashBytes[:])

	hostRows := sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
		AddRow(1, "Backup1", "backup1.local", "", root, []byte(`{}`), time.Now())
	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs("backup1.local").
		WillReturnRows(hostRows)

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\) FROM files\s+WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND hash IS NOT NULL.*AND hash NOT IN \('TIMEOUT_ERROR', 'HASH_ERROR'\)`).
		WithArgs("backup1.local").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	updateRe := `(?s)UPDATE files\s+SET hash = \$1, last_hashed_at = NOW\(\)\s+WHERE id = \$2`
	mock.ExpectExec(updateRe).
		WithArgs(changedFullHash, 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	fileRows := sqlmock.NewRows([]string{"id", "path", "root_folder", "hash"}).
		AddRow(1, "changed.bin", root, partialHash).
		AddRow(2, "same.bin", root, sameFullHash)
	mock.ExpectQuery(`(?s)SELECT id, path, root_folder, hash\s+FROM files\s+WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND hash IS NOT NULL.*AND hash NOT IN \('TIMEOUT_ERROR', 'HASH_ERROR'\).*AND id > \$2\s+ORDER BY id ASC\s+LIMIT 100`).
		WithArgs("backup1.local", 0).
		WillReturnRows(fileRows)

	if err := UpgradeStoredHashes(context.Background(), database, HashUpgradeOptions{Server: "backup1.local"}); err != nil {
		t.Fatalf("UpgradeStoredHashes error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
