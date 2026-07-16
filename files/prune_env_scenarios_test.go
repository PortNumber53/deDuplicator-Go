package files

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPruneRemovesMissingSymlinkAndDevicesInBatches(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	root := t.TempDir()
	symlinkTarget := filepath.Join(root, "target.txt")
	if err := os.WriteFile(symlinkTarget, []byte("x"), 0644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	symlinkPath := filepath.Join(root, "link.txt")
	if err := os.Symlink(symlinkTarget, symlinkPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	fifo := filepath.Join(root, "fifo")
	if err := syscall.Mkfifo(fifo, 0644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "HostA", lower, "", root, []byte(`{}`), time.Now()))

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM files WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	mock.ExpectQuery(`SELECT id, path, root_folder FROM files WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder"}).
			AddRow(1, "missing.txt", sql.NullString{String: root, Valid: true}).
			AddRow(2, "link.txt", sql.NullString{String: root, Valid: true}).
			AddRow(3, "fifo", sql.NullString{String: root, Valid: true}))

	mock.ExpectBegin()
	prep := mock.ExpectPrepare(`DELETE FROM files`)
	prep.ExpectExec().WithArgs(1).WillReturnResult(sqlmock.NewResult(0, 1))
	prep.ExpectExec().WithArgs(2).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	prep2 := mock.ExpectPrepare(`DELETE FROM files`)
	prep2.ExpectExec().WithArgs(3).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := PruneNonExistentFiles(context.Background(), db, PruneOptions{BatchSize: 2}); err != nil {
		t.Fatalf("PruneNonExistentFiles error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPruneHonorsEnvironmentLocalLimit(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	t.Setenv("ENVIRONMENT", "local")

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "HostA", lower, "", "/", []byte(`{}`), time.Now()))

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\) FROM files WHERE LOWER\(hostname\) = LOWER\(\$1\) LIMIT \d+`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	if err := PruneNonExistentFiles(context.Background(), db, PruneOptions{}); err != nil {
		t.Fatalf("PruneNonExistentFiles error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPruneTrustsRootFolderAndRemovesDuplicateResolvedPaths(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "movies"), 0755); err != nil {
		t.Fatalf("mkdir movies: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "movies", "movie.mkv"), []byte("x"), 0644); err != nil {
		t.Fatalf("write movie: %v", err)
	}
	stalePathFromRoot := strings.TrimPrefix(filepath.Join(root, "movies", "movie.mkv"), string(os.PathSeparator))

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)
	settings := []byte(`{"paths":{"Plex":` + strconv.Quote(root) + `}}`)

	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "HostA", lower, "", "/", settings, time.Now()))

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM files WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	mock.ExpectQuery(`SELECT id, path, root_folder FROM files WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder"}).
			AddRow(1, "movies/movie.mkv", sql.NullString{String: root, Valid: true}).
			AddRow(2, stalePathFromRoot, sql.NullString{String: "/", Valid: true}).
			AddRow(3, "movies/movie.mkv", sql.NullString{}))

	mock.ExpectBegin()
	prep := mock.ExpectPrepare(`DELETE FROM files`)
	prep.ExpectExec().WithArgs(2).WillReturnResult(sqlmock.NewResult(0, 1))
	prep.ExpectExec().WithArgs(3).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := PruneNonExistentFiles(context.Background(), db, PruneOptions{}); err != nil {
		t.Fatalf("PruneNonExistentFiles error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPruneCancellationStopsMidRun(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	root := t.TempDir()

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "HostA", lower, "", root, []byte(`{}`), time.Now()))

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM files WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	mock.ExpectQuery(`SELECT id, path, root_folder FROM files WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder"}).
			AddRow(1, "gone1", sql.NullString{String: root, Valid: true}).
			AddRow(2, "gone2", sql.NullString{String: root, Valid: true}))

	mock.ExpectBegin()
	prep := mock.ExpectPrepare(`DELETE FROM files`)
	prep.ExpectExec().WithArgs(1).WillDelayFor(10 * time.Millisecond).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectPrepare(`DELETE FROM files`)
	mock.ExpectRollback()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	if err := PruneNonExistentFiles(ctx, db, PruneOptions{BatchSize: 1}); err == nil {
		t.Fatalf("expected cancellation error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
