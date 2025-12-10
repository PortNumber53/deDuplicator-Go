package files

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"deduplicator/logging"

	"io"
	"log"

	"github.com/DATA-DOG/go-sqlmock"
)

func writeStub(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0755); err != nil {
		t.Fatalf("write stub %s: %v", name, err)
	}
	return path
}

func TestImportWarnsOnMissingFriendlyPathMapping(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "file.txt"), []byte("data"), 0644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT name, ip, root_path FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"name", "ip", "root_path"}).AddRow("Backup1", "", "/backups"))

	mock.ExpectQuery("SELECT hostname FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow(lower))

	mock.ExpectQuery("SELECT id, name, hostname, root_path, settings FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "root_path", "settings"}).
			AddRow(1, "Backup1", lower, "/backups", []byte(`{}`)))

	logging.InfoLogger = log.New(io.Discard, "", 0)
	logging.ErrorLogger = log.New(io.Discard, "", 0)

	orig := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	err = ImportFiles(context.Background(), db, ImportOptions{
		SourcePath:   source,
		HostName:     "Backup1",
		FriendlyPath: "photos",
		DryRun:       true,
	})
	_ = w.Close()
	os.Stdout = orig
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	if err != nil {
		t.Fatalf("ImportFiles missing mapping error: %v", err)
	}

	if !strings.Contains(buf.String(), "No path mapping") {
		t.Fatalf("expected warning about missing mapping, got: %s", buf.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMirrorFriendlyPathCopiesMissingAndFlagsConflicts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	hostAPath := t.TempDir()
	hostBPath := t.TempDir()

	hostname, _ := os.Hostname()
	localHost := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT name, hostname, root_path, settings FROM hosts").
		WillReturnRows(sqlmock.NewRows([]string{"name", "hostname", "root_path", "settings"}).
			AddRow("HostA", localHost, "", []byte(`{"paths":{"photos":"`+hostAPath+`"}}`)).
			AddRow("HostB", "remote.local", "", []byte(`{"paths":{"photos":"`+hostBPath+`"}}`)))

	mock.ExpectQuery("SELECT path, hash FROM files WHERE hostname = \\$1 AND root_folder = \\$2 AND hash IS NOT NULL").
		WithArgs(localHost, hostAPath).
		WillReturnRows(sqlmock.NewRows([]string{"path", "hash"}).
			AddRow("missing.jpg", "hash1").
			AddRow("conflict.txt", "hashX"))

	mock.ExpectQuery("SELECT path, hash FROM files WHERE hostname = \\$1 AND root_folder = \\$2 AND hash IS NOT NULL").
		WithArgs("remote.local", hostBPath).
		WillReturnRows(sqlmock.NewRows([]string{"path", "hash"}).
			AddRow("conflict.txt", "hashY"))

	stubDir := t.TempDir()
	writeStub(t, stubDir, "ssh", "#!/bin/sh\nif [ \"$2\" = \"test\" ]; then exit 1; fi\nexit 0\n")
	writeStub(t, stubDir, "rsync", "#!/bin/sh\nexit 0\n")
	t.Setenv("PATH", stubDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	var infoBuf, errBuf bytes.Buffer
	logging.InfoLogger = log.New(&infoBuf, "", 0)
	logging.ErrorLogger = log.New(&errBuf, "", 0)

	if err := MirrorFriendlyPath(context.Background(), db, "photos"); err != nil {
		t.Fatalf("MirrorFriendlyPath error: %v", err)
	}

	if !strings.Contains(errBuf.String(), "hash mismatch") {
		t.Fatalf("expected conflict logged for mismatched hashes, got: %s", errBuf.String())
	}
	if !strings.Contains(infoBuf.String(), "Files copied") && !strings.Contains(infoBuf.String(), "copied") {
		t.Fatalf("expected copy log for missing file, got: %s", infoBuf.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestImportSkipsExistingAndHashesNewFiles(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	source := t.TempDir()
	destRoot := filepath.Join(t.TempDir(), "dest")
	if err := os.MkdirAll(destRoot, 0755); err != nil {
		t.Fatalf("mkdir dest: %v", err)
	}

	existing := filepath.Join(source, "existing.txt")
	newFile := filepath.Join(source, "new.txt")
	if err := os.WriteFile(existing, []byte("dup"), 0644); err != nil {
		t.Fatalf("write existing: %v", err)
	}
	if err := os.WriteFile(newFile, []byte("fresh"), 0644); err != nil {
		t.Fatalf("write new: %v", err)
	}
	// place a copy at destination to simulate conflict
	if err := os.MkdirAll(destRoot, 0755); err != nil {
		t.Fatalf("mkdir dest root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destRoot, "existing.txt"), []byte("dup"), 0644); err != nil {
		t.Fatalf("write dest existing: %v", err)
	}

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT name, ip, root_path FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"name", "ip", "root_path"}).AddRow("Backup1", "", "/backups"))

	mock.ExpectQuery("SELECT hostname FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow(lower))

	mock.ExpectQuery("SELECT id, name, hostname, root_path, settings FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "root_path", "settings"}).
			AddRow(1, "Backup1", lower, "/backups", []byte(`{"paths":{"photos":"`+destRoot+`"}}`)))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM files WHERE hash = \\$1 AND hostname = \\$2").
		WithArgs(sqlmock.AnyArg(), "Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectExec("INSERT INTO files").
		WithArgs(filepath.Join(destRoot, "new.txt"), int64(len("fresh")), sqlmock.AnyArg(), lower).
		WillReturnResult(sqlmock.NewResult(1, 1))

	stubDir := t.TempDir()
	rsyncScript := `#!/bin/sh
remove=false
for a in "$@"; do
  if [ "$a" = "--remove-source-files" ]; then remove=true; fi
done
count=$#
src=$(eval echo \${$((count-1))})
dst=$(eval echo \${$count})
case "$dst" in
  *:*) exit 0;;
esac
mkdir -p "$(dirname "$dst")"
cp "$src" "$dst"
if $remove; then rm -f "$src"; fi
exit 0
`
	writeStub(t, stubDir, "rsync", rsyncScript)
	t.Setenv("PATH", stubDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	err = ImportFiles(context.Background(), db, ImportOptions{
		SourcePath:   source,
		HostName:     "Backup1",
		FriendlyPath: "photos",
		DryRun:       false,
	})
	if err != nil {
		t.Fatalf("ImportFiles error: %v", err)
	}

	if _, err := os.Stat(newFile); err != nil {
		t.Fatalf("expected source file to remain when remove-source is false: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destRoot, "new.txt")); err != nil {
		t.Fatalf("expected new file at destination: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestImportWithDuplicateDirMovesConflicts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	source := t.TempDir()
	destRoot := filepath.Join(t.TempDir(), "dest")
	dupDir := filepath.Join(t.TempDir(), "dupes")
	if err := os.MkdirAll(destRoot, 0755); err != nil {
		t.Fatalf("mkdir dest: %v", err)
	}
	conflict := filepath.Join(source, "file1.txt")
	if err := os.WriteFile(conflict, []byte("same"), 0644); err != nil {
		t.Fatalf("write conflict: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destRoot, "file1.txt"), []byte("same"), 0644); err != nil {
		t.Fatalf("write dest conflict: %v", err)
	}

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT name, ip, root_path FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"name", "ip", "root_path"}).AddRow("Backup1", "", "/backups"))

	mock.ExpectQuery("SELECT hostname FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow(lower))

	mock.ExpectQuery("SELECT id, name, hostname, root_path, settings FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "root_path", "settings"}).
			AddRow(1, "Backup1", lower, "/backups", []byte(`{"paths":{"photos":"`+destRoot+`"}}`)))

	stubDir := t.TempDir()
	writeStub(t, stubDir, "rsync", "#!/bin/sh\nexit 0\n")
	t.Setenv("PATH", stubDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	err = ImportFiles(context.Background(), db, ImportOptions{
		SourcePath:   source,
		HostName:     "Backup1",
		FriendlyPath: "photos",
		DryRun:       false,
		DuplicateDir: dupDir,
	})
	if err != nil {
		t.Fatalf("ImportFiles duplicate dir error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dupDir, "file1.txt")); err != nil {
		t.Fatalf("expected conflict file moved to duplicate dir: %v", err)
	}
	if _, err := os.Stat(conflict); !os.IsNotExist(err) {
		t.Fatalf("expected source conflict removed, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestImportAgeAndRemoveSourceRules(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	source := t.TempDir()
	destRoot := filepath.Join(t.TempDir(), "dest")
	if err := os.MkdirAll(destRoot, 0755); err != nil {
		t.Fatalf("mkdir dest: %v", err)
	}

	newer := filepath.Join(source, "newer.txt")
	older := filepath.Join(source, "older.txt")
	if err := os.WriteFile(newer, []byte("n"), 0644); err != nil {
		t.Fatalf("write newer: %v", err)
	}
	if err := os.WriteFile(older, []byte("o"), 0644); err != nil {
		t.Fatalf("write older: %v", err)
	}
	oldTime := time.Now().Add(-15 * time.Minute)
	if err := os.Chtimes(older, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes older: %v", err)
	}

	hostname, _ := os.Hostname()
	lower := strings.ToLower(hostname)

	mock.ExpectQuery("SELECT name, ip, root_path FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"name", "ip", "root_path"}).AddRow("Backup1", "", "/backups"))

	mock.ExpectQuery("SELECT hostname FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow(lower))

	mock.ExpectQuery("SELECT id, name, hostname, root_path, settings FROM hosts WHERE LOWER\\(name\\) = LOWER\\(\\$1\\)").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "root_path", "settings"}).
			AddRow(1, "Backup1", lower, "/backups", []byte(`{"paths":{"photos":"`+destRoot+`"}}`)))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM files WHERE hash = \\$1 AND hostname = \\$2").
		WithArgs(sqlmock.AnyArg(), "Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectExec("INSERT INTO files").
		WithArgs(filepath.Join(destRoot, "older.txt"), int64(1), sqlmock.AnyArg(), lower).
		WillReturnResult(sqlmock.NewResult(1, 1))

	stubDir := t.TempDir()
	rsyncScript := `#!/bin/sh
remove=false
for a in "$@"; do
  if [ "$a" = "--remove-source-files" ]; then remove=true; fi
done
count=$#
src=$(eval echo \${$((count-1))})
dst=$(eval echo \${$count})
mkdir -p "$(dirname "$dst")"
cp "$src" "$dst"
if $remove; then rm -f "$src"; fi
exit 0
`
	writeStub(t, stubDir, "rsync", rsyncScript)
	t.Setenv("PATH", stubDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	err = ImportFiles(context.Background(), db, ImportOptions{
		SourcePath:   source,
		HostName:     "Backup1",
		FriendlyPath: "photos",
		RemoveSource: true,
		DryRun:       false,
		Age:          10,
	})
	if err != nil {
		t.Fatalf("ImportFiles age/remove error: %v", err)
	}

	if _, err := os.Stat(newer); err != nil {
		t.Fatalf("expected newer file to remain at source: %v", err)
	}
	if _, err := os.Stat(older); !os.IsNotExist(err) {
		t.Fatalf("expected older file removed from source, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
