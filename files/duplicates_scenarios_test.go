package files

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"deduplicator/logging"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestFindDuplicateGroupsRespectsFiltersAndOrdering(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("hostname: %v", err)
	}
	lower := strings.ToLower(hostname)

	mock.ExpectQuery(`SELECT hostname FROM hosts WHERE LOWER\(hostname\) = LOWER\(\$1\)`).
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow("host-a"))

	dupRows := sqlmock.NewRows([]string{"hash", "path", "hostname", "size"}).
		AddRow("hash-b", "/data/b1", "host-a", int64(2*1024*1024)).
		AddRow("hash-b", "/data/b2", "host-a", int64(2*1024*1024)).
		AddRow("hash-a", "/data/a1", "host-a", int64(1024*1024)).
		AddRow("hash-a", "/data/a2", "host-a", int64(1024*1024))

	mock.ExpectQuery(`(?s)WITH duplicates.*size >= \$2.*LIMIT \$3.*JOIN files.*ORDER BY d.total_size DESC`).
		WithArgs("host-a", int64(1048576), 2).
		WillReturnRows(dupRows)

	groups, err := FindDuplicateGroups(context.Background(), db, lower, 1048576, 2)
	if err != nil {
		t.Fatalf("FindDuplicateGroups error: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if groups[0].Hash != "hash-b" || groups[0].TotalSize <= groups[1].TotalSize {
		t.Fatalf("expected groups ordered by total size with hash-b first, got %+v then %+v", groups[0], groups[1])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDedupFilesDryRunSkipsMoves(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	tempDir := t.TempDir()
	destDir := filepath.Join(tempDir, "dupes")
	if err := os.MkdirAll(filepath.Dir(destDir), 0755); err != nil {
		t.Fatalf("mkdir parent: %v", err)
	}

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT hostname FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow("host-a"))

	mock.ExpectQuery("WITH duplicates AS").
		WillReturnRows(sqlmock.NewRows([]string{"hash", "path", "hostname", "size"}).
			AddRow("h", "a/file1.txt", "host-a", int64(10)).
			AddRow("h", "a/file2.txt", "host-a", int64(10)))

	mock.ExpectQuery("SELECT root_path").
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"root_path"}).AddRow(tempDir))

	err = DedupFiles(context.Background(), db, DedupeOptions{
		DryRun:        true,
		DestDir:       destDir,
		StripPrefix:   "",
		Count:         0,
		IgnoreDestDir: false,
		MinSize:       0,
	})
	if err != nil {
		t.Fatalf("DedupFiles dry-run error: %v", err)
	}

	if _, statErr := os.Stat(destDir); !os.IsNotExist(statErr) {
		t.Fatalf("expected destination not to be created in dry-run, stat err: %v", statErr)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDedupFilesMovesAndDeletesFromDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	root := t.TempDir()
	dest := filepath.Join(root, "dest")
	if err := os.Mkdir(dest, 0755); err != nil {
		t.Fatalf("mkdir dest: %v", err)
	}

	keepDir := filepath.Join(root, "keepdir")
	moveDir := filepath.Join(root, "movedir")
	if err := os.MkdirAll(keepDir, 0755); err != nil {
		t.Fatalf("mkdir keepdir: %v", err)
	}
	if err := os.MkdirAll(moveDir, 0755); err != nil {
		t.Fatalf("mkdir movedir: %v", err)
	}

	keepFile := filepath.Join(keepDir, "dup.txt")
	moveFile := filepath.Join(moveDir, "dup.txt")
	extra := filepath.Join(keepDir, "other.txt")
	for _, path := range []string{keepFile, moveFile, extra} {
		if err := os.WriteFile(path, []byte("same"), 0644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT hostname FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow("host-a"))

	mock.ExpectQuery("WITH duplicates AS").
		WillReturnRows(sqlmock.NewRows([]string{"hash", "path", "hostname", "size"}).
			AddRow("hash1", strings.TrimPrefix(moveFile, root+string(os.PathSeparator)), "host-a", int64(4)).
			AddRow("hash1", strings.TrimPrefix(keepFile, root+string(os.PathSeparator)), "host-a", int64(4)))

	mock.ExpectQuery("SELECT root_path").
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"root_path"}).AddRow(root))

	mock.ExpectExec("DELETE FROM files").
		WithArgs(strings.TrimPrefix(moveFile, root+string(os.PathSeparator)), "host-a").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = DedupFiles(context.Background(), db, DedupeOptions{
		DryRun:        false,
		DestDir:       dest,
		StripPrefix:   "",
		Count:         0,
		IgnoreDestDir: false,
		MinSize:       0,
	})
	if err != nil {
		t.Fatalf("DedupFiles run error: %v", err)
	}

	if _, err := os.Stat(moveFile); !os.IsNotExist(err) {
		t.Fatalf("expected moved file to be removed from source, got stat err: %v", err)
	}
	if _, err := os.Stat(keepFile); err != nil {
		t.Fatalf("expected keep file to remain: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, strings.TrimPrefix(moveFile, root+string(os.PathSeparator)))); err != nil {
		t.Fatalf("expected moved file to be under dest: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDedupFilesSkipsGroupWithDestPrefix(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	tempDir := t.TempDir()
	destDir := filepath.Join(tempDir, "dupes")

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT hostname FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow("host-a"))

	mock.ExpectQuery("WITH duplicates AS").
		WillReturnRows(sqlmock.NewRows([]string{"hash", "path", "hostname", "size"}).
			AddRow("hash1", filepath.Join(destDir, "inside.txt"), "host-a", int64(1)).
			AddRow("hash1", "/other/outside.txt", "host-a", int64(1)))

	mock.ExpectQuery("SELECT root_path").
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"root_path"}).AddRow(tempDir))

	err = DedupFiles(context.Background(), db, DedupeOptions{
		DryRun:        false,
		DestDir:       destDir,
		StripPrefix:   "",
		Count:         0,
		IgnoreDestDir: true,
		MinSize:       0,
	})
	if err != nil {
		t.Fatalf("DedupFiles ignore-dest error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMoveDuplicatesDryRunUsesRootFolderAndSkipsChanges(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	root := t.TempDir()
	source1 := filepath.Join(root, "file1.txt")
	source2 := filepath.Join(root, "file2.txt")
	if err := os.WriteFile(source1, []byte("dup"), 0644); err != nil {
		t.Fatalf("write %s: %v", source1, err)
	}
	if err := os.WriteFile(source2, []byte("dup"), 0644); err != nil {
		t.Fatalf("write %s: %v", source2, err)
	}

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT hostname FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(lower).
		WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow("host-a"))

	mock.ExpectQuery("WITH duplicate_hashes AS").
		WillReturnRows(sqlmock.NewRows([]string{"hash", "path", "hostname", "size", "root_folder"}).
			AddRow("hash-1", filepath.Base(source1), "host-a", int64(3), root).
			AddRow("hash-1", filepath.Base(source2), "host-a", int64(3), root))

	logging.InfoLogger = log.New(io.Discard, "", 0)
	logging.ErrorLogger = log.New(io.Discard, "", 0)

	err = MoveDuplicates(context.Background(), db, DuplicateListOptions{}, MoveOptions{
		TargetDir: filepath.Join(root, "dupes"),
		DryRun:    true,
		Count:     0,
	})
	if err != nil {
		t.Fatalf("MoveDuplicates dry-run error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, "dupes", filepath.Base(source1))); !os.IsNotExist(err) {
		t.Fatalf("expected no files to be moved in dry-run, stat err: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
