package files

import (
	"bytes"
	"context"
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
	members := []groupMirrorMember{
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
	members := []groupMirrorMember{
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
	members := []groupMirrorMember{
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
	mock.ExpectExec(`(?s)INSERT INTO files \(path, hostname, size, hash, root_folder, last_hashed_at\)`).
		WithArgs("albums/2020/photo.jpg", "pinky.local", int64(5), "hash-family", pinkyRoot).
		WillReturnResult(sqlmock.NewResult(1, 1))

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
	rows := sqlmock.NewRows([]string{"path", "hash", "size"})
	for _, loc := range locations {
		rows.AddRow(loc.Path, loc.Hash, loc.Size)
	}
	mock.ExpectQuery(`(?s)SELECT path, hash, size\s+FROM files\s+WHERE LOWER\(hostname\) = LOWER\(\$1\)\s+AND root_folder = \$2\s+AND hash IS NOT NULL`).
		WithArgs(hostname, root).
		WillReturnRows(rows)
}

func expectGroupMirrorNoIndexedPathConflict(mock sqlmock.Sqlmock, hostname, path, root string) {
	mock.ExpectQuery(`(?s)SELECT root_folder, hash\s+FROM files\s+WHERE LOWER\(hostname\) = LOWER\(\$1\)\s+AND path = \$2\s+AND COALESCE\(root_folder, ''\) <> \$3\s+LIMIT 1`).
		WithArgs(hostname, path, root).
		WillReturnRows(sqlmock.NewRows([]string{"root_folder", "hash"}))
}
