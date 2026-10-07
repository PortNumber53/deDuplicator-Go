package files

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"deduplicator/logging"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Cancel at the database operation itself, rather than racing setup with a timer.
type cancelArgument struct{ cancel context.CancelFunc }

func (a cancelArgument) Match(driver.Value) bool { a.cancel(); return true }

func cancellationDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, context.Context, context.CancelFunc) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return database, mock, ctx, cancel
}
func cancellationHost(root string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).AddRow(1, "test", "test", "", root, []byte(fmt.Sprintf(`{"paths":{"data":%q}}`, root)), time.Now())
}
func assertCancelled(t *testing.T, err error, mock sqlmock.Sqlmock) {
	t.Helper()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	// BeginTx may perform rollback in its cancellation goroutine.
	until := time.Now().Add(time.Second)
	for {
		err = mock.ExpectationsWereMet()
		if err == nil {
			return
		}
		if time.Now().After(until) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFileCommandsAlreadyCancelled(t *testing.T) {
	commands := map[string]func(context.Context, *sql.DB) error{
		"dedupe-group": func(ctx context.Context, d *sql.DB) error { return DeduplicateByGroup(ctx, d, GroupDedupeOptions{}) },
		"mirror-group": func(ctx context.Context, d *sql.DB) error { return MirrorGroup(ctx, d, GroupMirrorOptions{}) },
		"mirror":       func(ctx context.Context, d *sql.DB) error { return MirrorFriendlyPath(ctx, d, "data") },
		"find":         func(ctx context.Context, d *sql.DB) error { return FindFiles(ctx, d, FindOptions{}) },
		"prune":        func(ctx context.Context, d *sql.DB) error { return PruneNonExistentFiles(ctx, d, PruneOptions{}) },
		"import":       func(ctx context.Context, d *sql.DB) error { return ImportFiles(ctx, d, ImportOptions{}) },
		"move": func(ctx context.Context, d *sql.DB) error {
			return MoveDuplicates(ctx, d, DuplicateListOptions{}, MoveOptions{})
		},
		"dedupe":  func(ctx context.Context, d *sql.DB) error { return DedupFiles(ctx, d, DedupeOptions{}) },
		"list":    func(ctx context.Context, d *sql.DB) error { return FindDuplicates(ctx, d, DuplicateListOptions{}) },
		"upgrade": func(ctx context.Context, d *sql.DB) error { return UpgradeStoredHashes(ctx, d, HashUpgradeOptions{}) },
		"stdin":   ProcessStdin,
		"workers": func(ctx context.Context, d *sql.DB) error { return ProcessFiles(ctx, d, "", FindOptions{}) },
	}
	for name, run := range commands {
		t.Run(name, func(t *testing.T) {
			d, m, ctx, cancel := cancellationDB(t)
			cancel()
			assertCancelled(t, run(ctx, d), m)
		})
	}
}

func TestDedupeCancellationDoesNotVisitRemainingCandidates(t *testing.T) {
	d, m, ctx, cancel := cancellationDB(t)
	members := familyScenarioMembers()
	expectGroupDedupeSetup(m, "family", 3, members)
	candidates := sqlmock.NewRows([]string{"hash", "size", "count", "total_size"})
	for i := 0; i < 1000; i++ {
		candidates.AddRow(fmt.Sprint(i), 10, 2, 20)
	}
	m.ExpectQuery("HAVING COUNT").WillReturnRows(candidates)
	m.ExpectQuery("WHERE f.hash").WithArgs(cancelArgument{cancel}, int64(10)).WillDelayFor(time.Second).WillReturnRows(sqlmock.NewRows([]string{"hash", "path", "hostname", "root_folder", "size"}))
	var logs bytes.Buffer
	old := logging.ErrorLogger
	logging.ErrorLogger = log.New(&logs, "", 0)
	defer func() { logging.ErrorLogger = old }()
	var err error
	output := captureStdout(t, func() { err = DeduplicateByGroup(ctx, d, GroupDedupeOptions{GroupName: "family", Verbose: true}) })
	assertCancelled(t, err, m)
	if logs.Len() != 0 || strings.Contains(output, "candidate 2/") {
		t.Fatalf("continued after cancellation: %s %s", logs.String(), output)
	}
}

func TestFindCancellationRollsBackPendingBatch(t *testing.T) {
	d, m, ctx, cancel := cancellationDB(t)
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "first"), []byte("a"), 0600)
	os.WriteFile(filepath.Join(root, "second"), []byte("b"), 0600)
	m.ExpectQuery("FROM hosts").WillReturnRows(cancellationHost(root))
	m.ExpectBegin()
	m.ExpectPrepare("INSERT INTO files").ExpectExec().WithArgs(cancelArgument{cancel}, "test", int64(1), root).WillDelayFor(time.Second).WillReturnResult(sqlmock.NewResult(1, 1))
	m.ExpectRollback()
	assertCancelled(t, FindFiles(ctx, d, FindOptions{Server: "test"}), m)
}

func TestHashUpgradeCancellationStopsUpdates(t *testing.T) {
	d, m, ctx, cancel := cancellationDB(t)
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "first"), []byte("a"), 0600)
	m.ExpectQuery("FROM hosts").WillReturnRows(cancellationHost(root))
	m.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	m.ExpectQuery("SELECT id, path").WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder", "hash"}).AddRow(1, "first", root, "old").AddRow(2, "second", root, "old")).RowsWillBeClosed()
	m.ExpectExec("UPDATE files").WithArgs(sqlmock.AnyArg(), cancelArgument{cancel}).WillDelayFor(time.Second).WillReturnResult(sqlmock.NewResult(0, 1))
	assertCancelled(t, UpgradeStoredHashes(ctx, d, HashUpgradeOptions{Server: "test"}), m)
}

func TestProcessStdinCancellationUnblocksRead(t *testing.T) {
	d, m, ctx, cancel := cancellationDB(t)
	m.ExpectQuery("SELECT name").WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("test"))
	m.ExpectBegin()
	m.ExpectPrepare("INSERT INTO files")
	m.ExpectRollback()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	old := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = old }()
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()
	assertCancelled(t, ProcessStdin(ctx, d), m)
}

func TestProcessFilesCancellationReleasesFullQueues(t *testing.T) {
	d, m, ctx, cancel := cancellationDB(t)
	root := t.TempDir()
	for i := 0; i < 100; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprint(i)), []byte("a"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	m.ExpectQuery("SELECT name").WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("test"))
	m.ExpectQuery("SELECT id, name, root_path").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "root_path"}).AddRow(1, "test", root))
	m.ExpectBegin()
	m.ExpectPrepare("INSERT INTO files")
	// Wait long enough for producer and result channels to fill before cancellation.
	m.ExpectPrepare("SELECT hash").ExpectQuery().WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillDelayFor(time.Second).WillReturnRows(sqlmock.NewRows([]string{"hash", "size", "mod_time"}))
	m.ExpectRollback()
	timer := time.AfterFunc(150*time.Millisecond, cancel)
	defer timer.Stop()
	start := time.Now()
	err := ProcessFiles(ctx, d, root, FindOptions{NumWorkers: 1})
	assertCancelled(t, err, m)
	if time.Since(start) > 2*time.Second {
		t.Fatal("workers did not shut down promptly")
	}
}

func TestHashCancellationIsNotInactivityTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := calculateFileHashContext(ctx, "unused")
	if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "timed out") {
		t.Fatalf("wrong cancellation: %v", err)
	}
}

func TestMirrorTransferCancellation(t *testing.T) {
	// exec replaces the stub process, so CommandContext can terminate the transfer.
	stub := t.TempDir()
	ready := filepath.Join(stub, "ready")
	writeStub(t, stub, "rsync", "#!/bin/sh\necho ready > \"$SHUTDOWN_READY\"\nexec sleep 30\n")
	t.Setenv("PATH", stub+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SHUTDOWN_READY", ready)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGroupMirrorRsync(ctx, "source", "destination") }()
	deadline := time.After(2 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("transfer did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transfer did not stop")
	}
}

func TestActiveHashShutdownAndInactivityRemainDistinct(t *testing.T) {
	for _, shutdown := range []bool{true, false} {
		t.Run(fmt.Sprint(shutdown), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			finished := make(chan struct{})
			reader := func(ctx context.Context, _ string, _ chan struct{}) (string, error) {
				close(started)
				<-ctx.Done()
				close(finished)
				return "", ctx.Err()
			}
			if shutdown {
				go func() { <-started; cancel() }()
			}
			_, err := runFileHash(ctx, "blocked", 20*time.Millisecond, reader)
			if shutdown {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), "hashing timed out") || errors.Is(err, context.Canceled) {
					t.Fatalf("expected inactivity timeout, got %v", err)
				}
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("hash worker did not finish")
			}
		})
	}
}

func TestImportCancellationBeforeTransfer(t *testing.T) {
	d, m, ctx, cancel := cancellationDB(t)
	source := t.TempDir()
	dest := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("a"), 0600); err != nil {
		t.Fatal(err)
	}
	hostname, _ := os.Hostname()
	m.ExpectQuery("SELECT name, ip, root_path").WillReturnRows(sqlmock.NewRows([]string{"name", "ip", "root_path"}).AddRow("test", "", dest))
	m.ExpectQuery("SELECT hostname").WillReturnRows(sqlmock.NewRows([]string{"hostname"}).AddRow(hostname))
	m.ExpectQuery("SELECT id, name, hostname, root_path, settings").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "root_path", "settings"}).AddRow(1, "test", hostname, dest, []byte(fmt.Sprintf(`{"paths":{"data":%q}}`, dest))))
	m.ExpectQuery("SELECT COUNT").WithArgs(cancelArgument{cancel}, hostname).WillDelayFor(time.Second).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	assertCancelled(t, ImportFiles(ctx, d, ImportOptions{SourcePath: source, HostName: "test", FriendlyPath: "data", RemoveSource: true}), m)
	if _, err := os.Stat(filepath.Join(source, "file")); err != nil {
		t.Fatal("source was removed", err)
	}
	if entries, err := os.ReadDir(dest); err != nil || len(entries) != 0 {
		t.Fatal("destination changed", err)
	}
}

func TestMoveCancellationDoesNotMoveNextFile(t *testing.T) {
	d, m, ctx, cancel := cancellationDB(t)
	root := t.TempDir()
	dest := t.TempDir()
	for _, name := range []string{"a", "b", "c"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("a"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	group := duplicateMoveGroup{Hash: "hash", Size: 1, Files: []string{"a", "b", "c"}, Hosts: []string{"test", "test", "test"}, RootPaths: []string{root, root, root}}
	m.ExpectExec("DELETE FROM files").WithArgs(cancelArgument{cancel}, "test", root).WillDelayFor(time.Second).WillReturnResult(sqlmock.NewResult(0, 1))
	_, err := moveGroupDuplicatesContext(ctx, group, MoveOptions{TargetDir: dest}, d, "test")
	assertCancelled(t, err, m)
	for _, name := range []string{"a", "c"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("unexpected move of %s: %v", name, err)
		}
	}
}

func TestGroupDedupeCancelledReplicationKeepsAllOriginals(t *testing.T) {
	d, m, ctx, cancel := cancellationDB(t)
	hostname, _ := os.Hostname()
	root := t.TempDir()
	for _, name := range []string{"a", "b"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("a"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	members := []groupMember{
		{Index: 0, Hostname: hostname, HostName: "Local", RootFolder: root, FriendlyPath: "data"},
		{Index: 1, Hostname: "remote", HostName: "Remote", RootFolder: "/data", FriendlyPath: "data"},
	}
	var locations []FileLocation
	for _, name := range []string{"a", "b"} {
		locations = append(locations, FileLocation{Hash: "hash", Size: 1, Path: name, Hostname: hostname, HostName: "Local", RootFolder: root, MemberIndex: 0})
	}
	m.ExpectQuery("SELECT root_folder, hash").WithArgs(cancelArgument{cancel}, sqlmock.AnyArg(), "/data").WillDelayFor(time.Second).WillReturnRows(sqlmock.NewRows([]string{"root_folder", "hash"}))
	_, err := processGroupDuplicates(ctx, d, locations, members, 2, GroupDedupeOptions{})
	assertCancelled(t, err, m)
	for _, name := range []string{"a", "b"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("original %s removed: %v", name, err)
		}
	}
}
