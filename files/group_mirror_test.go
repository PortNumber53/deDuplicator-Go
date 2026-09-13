package files

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"deduplicator/logging"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestChooseGroupMirrorPathPrefersMostCommonPath(t *testing.T) {
	members := []groupMember{
		{Index: 0, HostName: "Brain", FriendlyPath: "Personal", FileCount: 10},
		{Index: 1, HostName: "PI4", FriendlyPath: "BKP_Media", FileCount: 50},
		{Index: 2, HostName: "Pinky", FriendlyPath: "Personal", FileCount: 20},
	}
	locations := []groupMirrorLocation{
		{Hash: "hash-a", Path: "albums/2020/photo.jpg", Size: 10, MemberIndex: 0},
		{Hash: "hash-a", Path: "imports/photo.jpg", Size: 10, MemberIndex: 1},
		{Hash: "hash-a", Path: "albums/2020/photo.jpg", Size: 10, MemberIndex: 2},
	}

	path, ok := chooseGroupMirrorPath(locations, members)
	if !ok {
		t.Fatal("expected path choice")
	}
	if path != "albums/2020/photo.jpg" {
		t.Fatalf("path = %q, want most common path", path)
	}
}

func TestChooseGroupMirrorPathUsesMostPopulatedMemberAsTieBreaker(t *testing.T) {
	members := []groupMember{
		{Index: 0, HostName: "Brain", FriendlyPath: "Personal", FileCount: 10},
		{Index: 1, HostName: "PI4", FriendlyPath: "BKP_Media", FileCount: 50},
		{Index: 2, HostName: "Pinky", FriendlyPath: "Personal", FileCount: 20},
	}
	locations := []groupMirrorLocation{
		{Hash: "hash-a", Path: "albums/photo.jpg", Size: 10, MemberIndex: 0},
		{Hash: "hash-a", Path: "media/photo.jpg", Size: 10, MemberIndex: 1},
		{Hash: "hash-a", Path: "backup/photo.jpg", Size: 10, MemberIndex: 2},
	}

	path, ok := chooseGroupMirrorPath(locations, members)
	if !ok {
		t.Fatal("expected path choice")
	}
	if path != "media/photo.jpg" {
		t.Fatalf("path = %q, want path from member with most files", path)
	}
}

func TestPlanGroupMirrorTasksSkipsOccupiedDestinationPath(t *testing.T) {
	members := []groupMember{
		{Index: 0, HostName: "Brain", FriendlyPath: "Personal", FileCount: 20},
		{Index: 1, HostName: "PI4", FriendlyPath: "BKP_Media", FileCount: 10},
	}
	hashLocations := map[string][]groupMirrorLocation{
		"hash-a": {
			{Hash: "hash-a", Path: "albums/photo.jpg", Size: 10, MemberIndex: 0},
		},
	}
	memberPathHashes := map[int]map[string]string{
		0: {"albums/photo.jpg": "hash-a"},
		1: {"albums/photo.jpg": "hash-b"},
	}

	tasks, conflicts := planGroupMirrorTasks(hashLocations, members, memberPathHashes)
	if len(tasks) != 0 {
		t.Fatalf("expected no tasks for occupied destination path, got %+v", tasks)
	}
	if len(conflicts) != 1 || !strings.Contains(conflicts[0].Reason, "different hash") {
		t.Fatalf("expected occupied-path conflict, got %+v", conflicts)
	}
}

func TestMirrorGroupCopiesMissingHashAcrossDifferentFriendlyPaths(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()
	mock.MatchExpectationsInOrder(false)

	localHost, _ := os.Hostname()
	localHost = strings.ToLower(localHost)
	brainRoot := t.TempDir()
	piRoot := t.TempDir()
	pinkyRoot := t.TempDir()

	sourcePath := filepath.Join(brainRoot, "albums", "2020")
	if err := os.MkdirAll(sourcePath, 0755); err != nil {
		t.Fatalf("mkdir source path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "photo.jpg"), []byte("image"), 0644); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	mock.ExpectQuery(`(?s)SELECT id, name, description, min_copies, max_copies, created_at\s+FROM path_groups WHERE name = \$1`).
		WithArgs("family").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "description", "min_copies", "max_copies", "created_at"}).
			AddRow(1, "family", "Family files", 2, 3, time.Now()))

	mock.ExpectQuery(`(?s)SELECT pgm.id, pgm.group_id, pgm.host_name, pgm.friendly_path, pgm.priority\s+FROM path_group_members pgm`).
		WithArgs("family").
		WillReturnRows(sqlmock.NewRows([]string{"id", "group_id", "host_name", "friendly_path", "priority"}).
			AddRow(1, 1, "Brain", "Personal", 100).
			AddRow(2, 1, "PI4", "BKP_Media", 100).
			AddRow(3, 1, "Pinky", "Personal", 100))

	expectGroupMirrorHost(mock, "Brain", localHost, "Personal", brainRoot)
	expectGroupMirrorHost(mock, "PI4", "pi4.local", "BKP_Media", piRoot)
	expectGroupMirrorHost(mock, "Pinky", "pinky.local", "Personal", pinkyRoot)

	expectGroupMirrorCount(mock, localHost, brainRoot, 10)
	expectGroupMirrorCount(mock, "pi4.local", piRoot, 5)
	expectGroupMirrorCount(mock, "pinky.local", pinkyRoot, 0)

	expectGroupMirrorFiles(mock, localHost, brainRoot,
		[]groupMirrorLocation{{Path: "albums/2020/photo.jpg", Hash: "hash-family", Size: 5}})
	expectGroupMirrorFiles(mock, "pi4.local", piRoot,
		[]groupMirrorLocation{{Path: "camera/photo.jpg", Hash: "hash-family", Size: 5}})
	expectGroupMirrorFiles(mock, "pinky.local", pinkyRoot, nil)

	expectGroupMirrorNoIndexedPathConflict(mock, "pinky.local", "albums/2020/photo.jpg", pinkyRoot)
	mock.ExpectExec(`(?s)INSERT INTO files \(path, hostname, size, hash, root_folder, last_hashed_at, updated_at\)`).
		WithArgs("albums/2020/photo.jpg", "pinky.local", int64(5), "hash-family", pinkyRoot).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`(?s)UPDATE files SET updated_at = NOW\(\) WHERE hash = \$1`).
		WithArgs("hash-family", localHost, brainRoot, "pi4.local", piRoot, "pinky.local", pinkyRoot).
		WillReturnResult(sqlmock.NewResult(0, 3))

	stubDir := t.TempDir()
	writeStub(t, stubDir, "ssh", "#!/bin/sh\ncase \"$2\" in\n  test\\ -e*) exit 1;;\n  mkdir\\ -p*) exit 0;;\nesac\nexit 0\n")
	writeStub(t, stubDir, "rsync", "#!/bin/sh\nexit 0\n")
	t.Setenv("PATH", stubDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	var infoBuf, errBuf bytes.Buffer
	logging.InfoLogger = log.New(&infoBuf, "", 0)
	logging.ErrorLogger = log.New(&errBuf, "", 0)

	if err := MirrorGroup(context.Background(), database, GroupMirrorOptions{GroupName: "family"}); err != nil {
		t.Fatalf("MirrorGroup error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v\ninfo:\n%s\nerrors:\n%s", err, infoBuf.String(), errBuf.String())
	}
}

func TestOrderedGroupMirrorHashesPrioritizesNeverVisitedThenOldest(t *testing.T) {
	now := time.Now()
	hashLocations := map[string][]groupMirrorLocation{
		"hash-new": {
			{UpdatedAt: sql.NullTime{Time: now.Add(-time.Hour), Valid: true}},
		},
		"hash-never": {
			{UpdatedAt: sql.NullTime{}},
		},
		"hash-old": {
			{UpdatedAt: sql.NullTime{Time: now.Add(-2 * time.Hour), Valid: true}},
		},
		"hash-mixed": {
			{UpdatedAt: sql.NullTime{Time: now, Valid: true}},
			{UpdatedAt: sql.NullTime{Time: now.Add(-3 * time.Hour), Valid: true}},
		},
	}

	got := orderedGroupMirrorHashes(hashLocations)
	want := []string{"hash-never", "hash-mixed", "hash-old", "hash-new"}
	if len(got) != len(want) {
		t.Fatalf("ordered hashes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ordered hashes = %v, want %v", got, want)
		}
	}
}

func expectGroupMirrorHost(mock sqlmock.Sqlmock, name, hostname, friendlyPath, root string) {
	mock.ExpectQuery(`(?s)SELECT id, name, hostname, ip, root_path, settings, created_at\s+FROM hosts WHERE name = \$1`).
		WithArgs(name).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, name, hostname, "", root, []byte(fmt.Sprintf(`{"paths":{%q:%q}}`, friendlyPath, root)), time.Now()))
}

func expectGroupMirrorCount(mock sqlmock.Sqlmock, hostname, root string, count int64) {
	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\)\s+FROM files\s+WHERE LOWER\(hostname\) = LOWER\(\$1\)\s+AND root_folder = \$2`).
		WithArgs(hostname, root).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
}

func expectGroupMirrorFiles(mock sqlmock.Sqlmock, hostname, root string, locations []groupMirrorLocation) {
	rows := sqlmock.NewRows([]string{"path", "hash", "size", "updated_at"})
	for _, loc := range locations {
		var updatedAt interface{}
		if loc.UpdatedAt.Valid {
			updatedAt = loc.UpdatedAt.Time
		}
		rows.AddRow(loc.Path, loc.Hash, loc.Size, updatedAt)
	}
	mock.ExpectQuery(`(?s)SELECT path, hash, size, updated_at\s+FROM files\s+WHERE LOWER\(hostname\) = LOWER\(\$1\)\s+AND root_folder = \$2\s+AND hash IS NOT NULL`).
		WithArgs(hostname, root).
		WillReturnRows(rows)
}

func TestMirrorGroupTouchesHashWhenNoCopiesAreMissing(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()
	mock.MatchExpectationsInOrder(false)

	mock.ExpectQuery(`(?s)SELECT id, name, description, min_copies, max_copies, created_at\s+FROM path_groups WHERE name = \$1`).
		WithArgs("balanced").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "description", "min_copies", "max_copies", "created_at"}).
			AddRow(1, "balanced", "Already mirrored", 2, nil, time.Now()))
	mock.ExpectQuery(`(?s)SELECT pgm.id, pgm.group_id, pgm.host_name, pgm.friendly_path, pgm.priority\s+FROM path_group_members pgm`).
		WithArgs("balanced").
		WillReturnRows(sqlmock.NewRows([]string{"id", "group_id", "host_name", "friendly_path", "priority"}).
			AddRow(1, 1, "First", "Data", 100).
			AddRow(2, 1, "Second", "Backup", 100))

	expectGroupMirrorHost(mock, "First", "first.example", "Data", "/first/data")
	expectGroupMirrorHost(mock, "Second", "second.example", "Backup", "/second/backup")
	expectGroupMirrorCount(mock, "first.example", "/first/data", 1)
	expectGroupMirrorCount(mock, "second.example", "/second/backup", 1)
	expectGroupMirrorFiles(mock, "first.example", "/first/data", []groupMirrorLocation{
		{Path: "same.bin", Hash: "balanced-hash", Size: 5},
	})
	expectGroupMirrorFiles(mock, "second.example", "/second/backup", []groupMirrorLocation{
		{Path: "same.bin", Hash: "balanced-hash", Size: 5},
	})
	mock.ExpectExec(`(?s)UPDATE files SET updated_at = NOW\(\) WHERE hash = \$1`).
		WithArgs("balanced-hash", "first.example", "/first/data", "second.example", "/second/backup").
		WillReturnResult(sqlmock.NewResult(0, 2))

	if err := MirrorGroup(context.Background(), database, GroupMirrorOptions{GroupName: "balanced"}); err != nil {
		t.Fatalf("MirrorGroup: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func expectGroupMirrorNoIndexedPathConflict(mock sqlmock.Sqlmock, hostname, path, root string) {
	mock.ExpectQuery(`(?s)SELECT root_folder, hash\s+FROM files\s+WHERE LOWER\(hostname\) = LOWER\(\$1\)\s+AND path = \$2\s+AND COALESCE\(root_folder, ''\) <> \$3\s+LIMIT 1`).
		WithArgs(hostname, path, root).
		WillReturnRows(sqlmock.NewRows([]string{"root_folder", "hash"}))
}

// rsync must receive the remote path unquoted. Quoting it made rsync 3.2.4 and
// newer, which protect args by default, treat the quotes as part of the file
// name and resolve the path relative to the remote home directory.
func TestCopyGroupMirrorFileSendsUnquotedRemotePath(t *testing.T) {
	stubDir := t.TempDir()
	argsFile := filepath.Join(stubDir, "rsync-args")
	writeStub(t, stubDir, "rsync", "#!/bin/sh\nfor a in \"$@\"; do echo \"$a\" >> "+argsFile+"; done\nexit 0\n")
	t.Setenv("PATH", stubDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	task := groupMirrorTask{
		Hash:      "hash-family",
		Size:      5,
		RelPath:   "PINKY/brain2/40000/i00025.avi",
		SrcMember: groupMember{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", RootFolder: "/personal/"},
		DstMember: groupMember{HostName: "Pinky", Hostname: "pinky", FriendlyPath: "Personal", RootFolder: "/personal/"},
	}

	if err := copyGroupMirrorFile(context.Background(), "brain", task); err != nil {
		t.Fatalf("copyGroupMirrorFile: %v", err)
	}

	recorded, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read recorded rsync args: %v", err)
	}
	args := strings.Split(strings.TrimSpace(string(recorded)), "\n")
	want := []string{"-a", "-s", "/personal/PINKY/brain2/40000/i00025.avi", "pinky:/personal/PINKY/brain2/40000/i00025.avi"}
	if len(args) != len(want) {
		t.Fatalf("rsync args = %q, want %q", args, want)
	}
	for i, arg := range args {
		if arg != want[i] {
			t.Fatalf("rsync arg %d = %q, want %q (full: %q)", i, arg, want[i], args)
		}
	}
}
