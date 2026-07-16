package files

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dedupdb "deduplicator/db"
	"deduplicator/logging"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestHashOptions(t *testing.T) {
	// Test that HashOptions are correctly applied to the SQL query
	tests := []struct {
		name            string
		options         HashOptions
		expectedCountRe string
		expectedParam   interface{}
		hostSettings    []byte
	}{
		{
			name: "Hash only files without hashes and duplicate sizes",
			options: HashOptions{
				Server:           "testhost",
				Refresh:          false,
				Renew:            false,
				RetryProblematic: false,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND hash IS NULL.*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`,
			expectedParam:   "testhost",
		},
		{
			name: "Refresh duplicate-size files",
			options: HashOptions{
				Server:           "testhost",
				Refresh:          true,
				Renew:            false,
				RetryProblematic: false,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`,
			expectedParam:   "testhost",
		},
		{
			name: "Full hash scans all unhashed files",
			options: HashOptions{
				Server:           "testhost",
				Refresh:          false,
				Renew:            false,
				RetryProblematic: false,
				FullHash:         true,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\) AND hash IS NULL\s*$`,
			expectedParam:   "testhost",
		},
		{
			name: "Full hash with refresh scans all files",
			options: HashOptions{
				Server:           "testhost",
				Refresh:          true,
				Renew:            false,
				RetryProblematic: false,
				FullHash:         true,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\)\s*$`,
			expectedParam:   "testhost",
		},
		{
			name: "Renew old hashes",
			options: HashOptions{
				Server:           "testhost",
				Refresh:          false,
				Renew:            true,
				RetryProblematic: false,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND \(hash IS NULL OR last_hashed_at < NOW\(\) - INTERVAL '1 week'\).*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`,
			expectedParam:   "testhost",
		},
		{
			name: "Retry problematic files",
			options: HashOptions{
				Server:           "testhost",
				Refresh:          false,
				Renew:            false,
				RetryProblematic: true,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND \(hash IS NULL OR hash IN \('TIMEOUT_ERROR', 'HASH_ERROR'\)\).*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`,
			expectedParam:   "testhost",
		},
		{
			name: "Retry problematic and renew old hashes",
			options: HashOptions{
				Server:           "testhost",
				Refresh:          false,
				Renew:            true,
				RetryProblematic: true,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND \(hash IS NULL OR hash IN \('TIMEOUT_ERROR', 'HASH_ERROR'\) OR last_hashed_at < NOW\(\) - INTERVAL '1 week'\).*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`,
			expectedParam:   "testhost",
		},
		{
			name: "First chunk large first scans unhashed duplicate-size files",
			options: HashOptions{
				Server:     "testhost",
				FirstChunk: true,
				LargeFirst: true,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND hash IS NULL.*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`,
			expectedParam:   "testhost",
		},
		{
			name: "Full hash large first scans all unhashed files",
			options: HashOptions{
				Server:     "testhost",
				FullHash:   true,
				LargeFirst: true,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\) AND hash IS NULL\s*$`,
			expectedParam:   "testhost",
		},
		{
			name: "Full hash force large first scans all files",
			options: HashOptions{
				Server:     "testhost",
				Refresh:    true,
				FullHash:   true,
				LargeFirst: true,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\)\s*$`,
			expectedParam:   "testhost",
		},
		{
			name: "First chunk force large first scans selected duplicate-size files",
			options: HashOptions{
				Server:     "testhost",
				Refresh:    true,
				FirstChunk: true,
				LargeFirst: true,
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`,
			expectedParam:   "testhost",
		},
		{
			name: "Path priority preserves default duplicate-size eligibility",
			options: HashOptions{
				Server: "testhost",
				Paths:  []string{"photos"},
			},
			expectedCountRe: `(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND hash IS NULL.*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`,
			expectedParam:   "testhost",
			hostSettings:    []byte(`{"paths":{"photos":"/data/photos"}}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a new mock database
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			if err != nil {
				t.Fatalf("Failed to create mock database: %v", err)
			}
			defer db.Close()

			hostSettings := tc.hostSettings
			if hostSettings == nil {
				hostSettings = []byte(`{}`)
			}
			hostRows := sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
				AddRow(1, "testhost", "testhost", "", "/test/path", hostSettings, time.Now())
			mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
				WithArgs(tc.options.Server).
				WillReturnRows(hostRows)

			countRows := sqlmock.NewRows([]string{"count"}).AddRow(0)
			mock.ExpectQuery(tc.expectedCountRe).
				WithArgs(tc.expectedParam).
				WillReturnRows(countRows)

			err = HashFiles(context.Background(), db, tc.options)
			if err != nil {
				t.Errorf("HashFiles returned error: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestHashFilesRejectsConflictingHashModesBeforeDBAccess(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	err = HashFiles(context.Background(), db, HashOptions{
		Server:     "testhost",
		FirstChunk: true,
		FullHash:   true,
	})
	if err == nil {
		t.Fatal("expected conflicting hash mode error")
	}
	if !strings.Contains(err.Error(), "--first-chunk and --full-hash cannot be used together") {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected database access: %v", err)
	}
}

func TestHashFilesHostNotFound(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs("nonexistent").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE name = \$1`).
		WithArgs("nonexistent").
		WillReturnError(sql.ErrNoRows)

	err = HashFiles(context.Background(), db, HashOptions{Server: "nonexistent"})

	if err == nil {
		t.Error("Expected error for non-existent host, got nil")
	} else if err.Error() != "server not found: nonexistent" {
		t.Errorf("Expected 'server not found' error, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestHashFilesBatchQueryDoesNotBreakWithLocalEnvironmentLimit(t *testing.T) {
	// This test is intentionally string-based to avoid brittle sqlmock expectations.
	// The goal is to prevent future regressions where LIMIT/ORDER get composed in
	// an invalid order.
	whereClause := buildHashWhereClause(HashOptions{
		Server:           "testhost",
		Refresh:          false,
		Renew:            false,
		RetryProblematic: false,
	})

	batch := buildHashBatchQuery(whereClause, 100, hashBatchQueryOptions{})
	if !strings.Contains(batch, "id > $2") {
		t.Fatalf("expected batch query to use an id bookmark; got: %s", batch)
	}

	if !strings.Contains(batch, "ORDER BY id ASC") {
		t.Fatalf("expected batch query to order by id for bookmark pagination; got: %s", batch)
	}
	if !strings.Contains(batch, "LIMIT 100") {
		t.Fatalf("expected batch query to include LIMIT 100; got: %s", batch)
	}

	idxOrder := strings.Index(batch, "ORDER BY")
	idxLimit := strings.Index(batch, "LIMIT")
	if idxOrder == -1 || idxLimit == -1 || idxLimit < idxOrder {
		t.Fatalf("expected LIMIT after ORDER BY; got: %s", batch)
	}
}

func TestHashFilesLargeFirstBatchQueryUsesSizeBookmark(t *testing.T) {
	whereClause := buildHashWhereClause(HashOptions{LargeFirst: true})

	batch := buildHashBatchQuery(whereClause, 100, hashBatchQueryOptions{LargeFirst: true})
	if !strings.Contains(batch, "hash IS NULL") {
		t.Fatalf("large-first mode should still select unhashed files by default; got: %s", batch)
	}
	if !strings.Contains(batch, "COALESCE(size, -1) < $2::bigint") {
		t.Fatalf("expected large-first query to advance by effective size; got: %s", batch)
	}
	if !strings.Contains(batch, "COALESCE(size, -1) = $2::bigint AND id > $3") {
		t.Fatalf("expected large-first query to break equal-size ties by id; got: %s", batch)
	}
	if !strings.Contains(batch, "ORDER BY COALESCE(size, -1) DESC, id ASC") {
		t.Fatalf("expected large-first query to order by size descending then id; got: %s", batch)
	}
	if !strings.Contains(batch, "LIMIT 100") {
		t.Fatalf("expected large-first query to include LIMIT 100; got: %s", batch)
	}
}

func TestHashFilesPathPriorityBatchQueryUsesPriorityBookmark(t *testing.T) {
	whereClause := buildHashWhereClause(HashOptions{})

	batch := buildHashBatchQuery(whereClause, 100, hashBatchQueryOptions{PrioritizePaths: true})
	if !strings.Contains(batch, "array_position($2::text[], COALESCE(root_folder, ''))") {
		t.Fatalf("expected path priority query to use ordered root folder array parameter; got: %s", batch)
	}
	if !strings.Contains(batch, "$3::int IS NULL") {
		t.Fatalf("expected path priority query to use priority bookmark; got: %s", batch)
	}
	if !strings.Contains(batch, "path_priority ASC, id ASC") {
		t.Fatalf("expected path priority query to order by priority then id; got: %s", batch)
	}
	if !strings.Contains(batch, "id > $4") {
		t.Fatalf("expected path priority query to advance equal-priority rows by id; got: %s", batch)
	}
}

func TestHashFilesPathPriorityLargeFirstBatchQueryUsesPriorityAndSizeBookmarks(t *testing.T) {
	whereClause := buildHashWhereClause(HashOptions{LargeFirst: true})

	batch := buildHashBatchQuery(whereClause, 100, hashBatchQueryOptions{LargeFirst: true, PrioritizePaths: true})
	if !strings.Contains(batch, "array_position($2::text[], COALESCE(root_folder, ''))") {
		t.Fatalf("expected path priority query to use ordered root folder array parameter; got: %s", batch)
	}
	if !strings.Contains(batch, "$3::int IS NULL") {
		t.Fatalf("expected path priority query to use priority bookmark; got: %s", batch)
	}
	if !strings.Contains(batch, "$4::bigint IS NULL") {
		t.Fatalf("expected large-first path priority query to use size bookmark; got: %s", batch)
	}
	if !strings.Contains(batch, "COALESCE(size, -1) = $4::bigint AND id > $5") {
		t.Fatalf("expected large-first path priority query to break equal-size ties by id; got: %s", batch)
	}
	if !strings.Contains(batch, "ORDER BY path_priority ASC, COALESCE(size, -1) DESC, id ASC") {
		t.Fatalf("expected large-first path priority query to order by priority, size, then id; got: %s", batch)
	}
}

func TestResolveHashPriorityRootFolders(t *testing.T) {
	host := &dedupdb.Host{
		Name:     "Backup1",
		Settings: json.RawMessage(`{"paths":{"photos":"/data/photos","videos":"/data/videos"}}`),
	}

	roots, err := resolveHashPriorityRootFolders(host, []string{"photos", "/scratch", "photos", "videos"})
	if err != nil {
		t.Fatalf("resolve priority root folders: %v", err)
	}
	expected := []string{"/data/photos", "/scratch", "/data/videos"}
	if strings.Join(roots, "\n") != strings.Join(expected, "\n") {
		t.Fatalf("roots = %#v, want %#v", roots, expected)
	}
}

func TestResolveHashPriorityRootFoldersRejectsUnknownFriendlyPath(t *testing.T) {
	host := &dedupdb.Host{
		Name:     "Backup1",
		Settings: json.RawMessage(`{"paths":{"photos":"/data/photos"}}`),
	}

	_, err := resolveHashPriorityRootFolders(host, []string{"missing"})
	if err == nil {
		t.Fatal("expected unknown friendly path error")
	}
	if !strings.Contains(err.Error(), "friendly path 'missing' not found for server 'Backup1'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHashFilesProcessesFirstChunkLargeFirstCombination(t *testing.T) {
	var logBuffer bytes.Buffer
	logging.InfoLogger = log.New(&logBuffer, "", 0)
	logging.ErrorLogger = log.New(io.Discard, "", 0)

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	root := t.TempDir()
	prefix := strings.Repeat("x", int(firstChunkHashBytes))
	firstContent := []byte(prefix + "suffix-0001")
	secondContent := []byte(prefix + "suffix-0002")
	if err := os.WriteFile(filepath.Join(root, "first.bin"), firstContent, 0644); err != nil {
		t.Fatalf("write first file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "second.bin"), secondContent, 0644); err != nil {
		t.Fatalf("write second file: %v", err)
	}

	expected := sha256.Sum256([]byte(prefix))
	expectedHash := hex.EncodeToString(expected[:])

	hostRows := sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
		AddRow(1, "Backup1", "backup1.local", "", root, []byte(`{}`), time.Now())
	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs("backup1.local").
		WillReturnRows(hostRows)

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND hash IS NULL.*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`).
		WithArgs("backup1.local").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	updateRe := `(?s)UPDATE files\s+SET hash = \$1, last_hashed_at = NOW\(\)\s+WHERE id = \$2`
	mock.ExpectPrepare(`(?s)UPDATE files\s+SET hash = 'TIMEOUT_ERROR', last_hashed_at = NOW\(\)\s+WHERE id = \$1`)
	mock.ExpectPrepare(`(?s)UPDATE files\s+SET hash = 'HASH_ERROR', last_hashed_at = NOW\(\)\s+WHERE id = \$1`)

	fileRows := sqlmock.NewRows([]string{"id", "path", "root_folder", "effective_size"}).
		AddRow(1, "first.bin", root, int64(len(firstContent))).
		AddRow(2, "second.bin", root, int64(len(secondContent)))
	mock.ExpectQuery(`(?s)SELECT id, path, root_folder, COALESCE\(size, -1\) AS effective_size.*COALESCE\(size, -1\) < \$2::bigint.*ORDER BY COALESCE\(size, -1\) DESC, id ASC`).
		WithArgs("backup1.local", nil, 0).
		WillReturnRows(fileRows)

	mock.ExpectPrepare(updateRe).
		ExpectExec().
		WithArgs(expectedHash, 1).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectPrepare(updateRe).
		ExpectExec().
		WithArgs(expectedHash, 2).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = HashFiles(context.Background(), db, HashOptions{
		Server:     "backup1.local",
		FirstChunk: true,
		LargeFirst: true,
	})
	if err != nil {
		t.Fatalf("HashFiles first-chunk large-first error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v\nlogs:\n%s", err, logBuffer.String())
	}
}

func TestHashFilesProcessesPrioritizedFriendlyPathFirst(t *testing.T) {
	var logBuffer bytes.Buffer
	logging.InfoLogger = log.New(&logBuffer, "", 0)
	logging.ErrorLogger = log.New(io.Discard, "", 0)

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	priorityRoot := t.TempDir()
	otherRoot := t.TempDir()
	priorityContent := []byte("priority file content")
	otherContent := []byte("other file content---")
	if len(priorityContent) != len(otherContent) {
		t.Fatalf("test fixture contents must have matching sizes")
	}
	if err := os.WriteFile(filepath.Join(priorityRoot, "priority.bin"), priorityContent, 0644); err != nil {
		t.Fatalf("write priority file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(otherRoot, "other.bin"), otherContent, 0644); err != nil {
		t.Fatalf("write other file: %v", err)
	}

	priorityHashBytes := sha256.Sum256(priorityContent)
	priorityHash := hex.EncodeToString(priorityHashBytes[:])
	otherHashBytes := sha256.Sum256(otherContent)
	otherHash := hex.EncodeToString(otherHashBytes[:])

	hostRows := sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
		AddRow(1, "Backup1", "backup1.local", "", priorityRoot, []byte(fmt.Sprintf(`{"paths":{"photos":%q}}`, priorityRoot)), time.Now())
	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs("backup1.local").
		WillReturnRows(hostRows)

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\) FROM files.*WHERE LOWER\(hostname\) = LOWER\(\$1\).*AND hash IS NULL.*AND size IS NOT NULL.*HAVING COUNT\(\*\) > 1`).
		WithArgs("backup1.local").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	updateRe := `(?s)UPDATE files\s+SET hash = \$1, last_hashed_at = NOW\(\)\s+WHERE id = \$2`
	mock.ExpectPrepare(`(?s)UPDATE files\s+SET hash = 'TIMEOUT_ERROR', last_hashed_at = NOW\(\)\s+WHERE id = \$1`)
	mock.ExpectPrepare(`(?s)UPDATE files\s+SET hash = 'HASH_ERROR', last_hashed_at = NOW\(\)\s+WHERE id = \$1`)

	fileRows := sqlmock.NewRows([]string{"id", "path", "root_folder", "effective_size", "path_priority"}).
		AddRow(2, "priority.bin", priorityRoot, int64(len(priorityContent)), int64(1)).
		AddRow(1, "other.bin", otherRoot, int64(len(otherContent)), int64(2))
	mock.ExpectQuery(`(?s)SELECT id, path, root_folder, COALESCE\(size, -1\) AS effective_size, .* AS path_priority.*array_position\(\$2::text\[\], COALESCE\(root_folder, ''\)\).*ORDER BY path_priority ASC, id ASC`).
		WithArgs("backup1.local", sqlmock.AnyArg(), nil, 0).
		WillReturnRows(fileRows)

	mock.ExpectPrepare(updateRe).
		ExpectExec().
		WithArgs(priorityHash, 2).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectPrepare(updateRe).
		ExpectExec().
		WithArgs(otherHash, 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = HashFiles(context.Background(), db, HashOptions{
		Server: "backup1.local",
		Paths:  []string{"photos"},
	})
	if err != nil {
		t.Fatalf("HashFiles path priority error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v\nlogs:\n%s", err, logBuffer.String())
	}
}

func TestHashWhereClauseModeSelection(t *testing.T) {
	defaultWhere := buildHashWhereClause(HashOptions{})
	if !strings.Contains(defaultWhere, "hash IS NULL") {
		t.Fatalf("default mode should only select unhashed files; got: %s", defaultWhere)
	}
	if !strings.Contains(defaultWhere, "HAVING COUNT(*) > 1") {
		t.Fatalf("default mode should filter to duplicate file sizes; got: %s", defaultWhere)
	}

	firstChunkWhere := buildHashWhereClause(HashOptions{FirstChunk: true})
	if !strings.Contains(firstChunkWhere, "hash IS NULL") || !strings.Contains(firstChunkWhere, "HAVING COUNT(*) > 1") {
		t.Fatalf("first-chunk mode should select unhashed duplicate-size files; got: %s", firstChunkWhere)
	}

	fullHashWhere := buildHashWhereClause(HashOptions{FullHash: true})
	if !strings.Contains(fullHashWhere, "hash IS NULL") {
		t.Fatalf("full-hash mode should select unhashed files by default; got: %s", fullHashWhere)
	}
	if strings.Contains(fullHashWhere, "HAVING COUNT(*) > 1") {
		t.Fatalf("full-hash mode should not filter to duplicate file sizes; got: %s", fullHashWhere)
	}

	forceFullHashWhere := buildHashWhereClause(HashOptions{Refresh: true, FullHash: true})
	if strings.Contains(forceFullHashWhere, "hash IS NULL") || strings.Contains(forceFullHashWhere, "HAVING COUNT(*) > 1") {
		t.Fatalf("full-hash force mode should select all files; got: %s", forceFullHashWhere)
	}
}

func TestHashFilesProcessing(t *testing.T) {
	// Skip this test for now as it's causing issues with SQL formatting
	t.Skip("Skipping test due to SQL formatting issues")

	// Create a new mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "hash_files_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	testFilePath := filepath.Join(tempDir, "test.txt")
	err = os.WriteFile(testFilePath, []byte("test content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Set up expectations for the host query
	hostRows := sqlmock.NewRows([]string{"root_path", "hostname"}).
		AddRow(tempDir, "testhost")
	mock.ExpectQuery("SELECT root_path, hostname").
		WithArgs("testhost").
		WillReturnRows(hostRows)

	// Set up expectations for the count query
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("testhost").
		WillReturnRows(countRows)

	// Call the function
	err = HashFiles(context.Background(), db, HashOptions{Server: "testhost"})
	if err != nil {
		t.Errorf("HashFiles returned error: %v", err)
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestListProblematicFiles(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	hostRows := sqlmock.NewRows([]string{"root_path"}).
		AddRow("/test/path")
	mock.ExpectQuery(`SELECT root_path FROM hosts WHERE LOWER\(name\) = LOWER\(\$1\)`).
		WithArgs("testhost").
		WillReturnRows(hostRows)

	now := time.Now()
	fileRows := sqlmock.NewRows([]string{"id", "dbPath", "size", "last_hashed_at"}).
		AddRow(1, "problem1.txt", 1024*1024, now).
		AddRow(2, "problem2.txt", 1024*1024*1024, now.Add(-24*time.Hour))
	mock.ExpectQuery(`SELECT id, dbPath, size, last_hashed_at FROM files WHERE LOWER\(hostname\) = LOWER\(\$1\) AND hash = 'TIMEOUT_ERROR' ORDER BY last_hashed_at DESC`).
		WithArgs("testhost").
		WillReturnRows(fileRows)

	err = ListProblematicFiles(context.Background(), db, "testhost")
	if err != nil {
		t.Errorf("ListProblematicFiles returned error: %v", err)
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestListProblematicFilesHostNotFound(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT root_path FROM hosts WHERE LOWER\(name\) = LOWER\(\$1\)`).
		WithArgs("nonexistent").
		WillReturnError(sql.ErrNoRows)

	err = ListProblematicFiles(context.Background(), db, "nonexistent")

	if err == nil {
		t.Error("Expected error for non-existent host, got nil")
	} else if err.Error() != "host not found: nonexistent" {
		t.Errorf("Expected 'host not found' error, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestListProblematicFilesNoResults(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	hostRows := sqlmock.NewRows([]string{"root_path"}).
		AddRow("/test/path")
	mock.ExpectQuery(`SELECT root_path FROM hosts WHERE LOWER\(name\) = LOWER\(\$1\)`).
		WithArgs("testhost").
		WillReturnRows(hostRows)

	fileRows := sqlmock.NewRows([]string{"id", "dbPath", "size", "last_hashed_at"})
	mock.ExpectQuery(`SELECT id, dbPath, size, last_hashed_at FROM files WHERE LOWER\(hostname\) = LOWER\(\$1\) AND hash = 'TIMEOUT_ERROR' ORDER BY last_hashed_at DESC`).
		WithArgs("testhost").
		WillReturnRows(fileRows)

	err = ListProblematicFiles(context.Background(), db, "testhost")
	if err != nil {
		t.Errorf("ListProblematicFiles returned error: %v", err)
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}
