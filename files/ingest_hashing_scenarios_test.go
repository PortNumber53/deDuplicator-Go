package files

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestProcessStdinOnlyInsertsRegularFiles(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	temp := t.TempDir()
	regular := filepath.Join(temp, "file.txt")
	dir := filepath.Join(temp, "dir")
	symlink := filepath.Join(temp, "link.txt")
	fifo := filepath.Join(temp, "fifo")
	if err := os.WriteFile(regular, []byte("ok"), 0644); err != nil {
		t.Fatalf("write regular: %v", err)
	}
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("mkdir dir: %v", err)
	}
	if err := os.Symlink(regular, symlink); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := syscall.Mkfifo(fifo, 0644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT name FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("host-row"))

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO files").
		ExpectExec().
		WithArgs(regular, "host-row", int64(len("ok"))).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	r, w, _ := os.Pipe()
	orig := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = orig }()

	go func() {
		defer w.Close()
		w.WriteString(regular + "\n")
		w.WriteString(dir + "\n")
		w.WriteString(symlink + "\n")
		w.WriteString(fifo + "\n")
	}()

	if err := ProcessStdin(context.Background(), db); err != nil {
		t.Fatalf("ProcessStdin error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestFindFilesStoresRootFolder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0644); err != nil {
		t.Fatalf("write a: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested.txt"), []byte("b"), 0644); err != nil {
		t.Fatalf("write nested: %v", err)
	}

	now := time.Now()
	mock.ExpectQuery("SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE name = \\$1").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "Backup1", "backup1.local", "1.1.1.1", "/old", []byte(`{"paths":{"photos":"`+root+`"}}`), now))

	mock.ExpectBegin()
	prep := mock.ExpectPrepare("INSERT INTO files")
	prep.ExpectExec().
		WithArgs("a.txt", "backup1.local", sqlmock.AnyArg(), root).
		WillReturnResult(sqlmock.NewResult(1, 1))
	prep.ExpectExec().
		WithArgs("nested.txt", "backup1.local", sqlmock.AnyArg(), root).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := FindFiles(context.Background(), db, FindOptions{Server: "Backup1", Path: "photos"}); err != nil {
		t.Fatalf("FindFiles error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHashFilesOnlyUnhashedByDefault(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	temp := t.TempDir()
	file := filepath.Join(temp, "file.txt")
	if err := os.WriteFile(file, []byte("hashme"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs("backup1.local").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "Backup1", "backup1.local", "", "/root", []byte(`{}`), time.Now()))

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\) FROM \(.*hash IS NULL`).
		WithArgs("backup1.local").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	if err := HashFiles(context.Background(), db, HashOptions{Server: "backup1.local"}); err != nil {
		t.Fatalf("HashFiles error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHashFilesRetriesProblematic(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	temp := t.TempDir()
	file := filepath.Join(temp, "retry.txt")
	if err := os.WriteFile(file, []byte("retry"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs("backup1.local").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "Backup1", "backup1.local", "", "/root", []byte(`{}`), time.Now()))

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\) FROM \(.*hash = 'TIMEOUT_ERROR'`).
		WithArgs("backup1.local").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	if err := HashFiles(context.Background(), db, HashOptions{Server: "backup1.local", RetryProblematic: true}); err != nil {
		t.Fatalf("HashFiles retry error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHashFilesForceRehashesExisting(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	temp := t.TempDir()
	file := filepath.Join(temp, "existing.txt")
	if err := os.WriteFile(file, []byte("rehash"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	mock.ExpectQuery(`SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs("backup1.local").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "Backup1", "backup1.local", "", "/root", []byte(`{}`), time.Now()))

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\) FROM \( SELECT id, path, root_folder FROM files`).
		WithArgs("backup1.local").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	if err := HashFiles(context.Background(), db, HashOptions{Server: "backup1.local", Refresh: true}); err != nil {
		t.Fatalf("HashFiles refresh error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHashFilesFailsWhenHostUnknown(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE name = \\$1").
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	if err := HashFiles(context.Background(), db, HashOptions{Server: "missing"}); err == nil {
		t.Fatalf("expected error for unknown host")
	}
}
