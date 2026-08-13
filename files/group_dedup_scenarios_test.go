package files

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type scenarioGroupMember struct {
	HostName     string
	Hostname     string
	FriendlyPath string
	RootFolder   string
	Priority     int
}

type scenarioFileCopy struct {
	Path       string
	Hostname   string
	RootFolder string
}

// expectGroupDedupeSetup mocks the group, member, and host lookups that every
// dedupe-group run performs before it queries for duplicates.
func expectGroupDedupeSetup(mock sqlmock.Sqlmock, groupName string, minCopies int, members []scenarioGroupMember) {
	groupRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"id", "name", "description", "min_copies", "max_copies", "created_at"}).
			AddRow(1, groupName, "Family files", minCopies, nil, time.Now())
	}
	// Once for the copy limits, once while resolving members.
	mock.ExpectQuery("FROM path_groups WHERE name = \\$1").WithArgs(groupName).WillReturnRows(groupRows())
	mock.ExpectQuery("FROM path_groups WHERE name = \\$1").WithArgs(groupName).WillReturnRows(groupRows())

	memberRows := sqlmock.NewRows([]string{"id", "group_id", "host_name", "friendly_path", "priority"})
	for i, member := range members {
		memberRows.AddRow(i+1, 1, member.HostName, member.FriendlyPath, member.Priority)
	}
	mock.ExpectQuery("FROM path_group_members pgm").WithArgs(groupName).WillReturnRows(memberRows)

	for i, member := range members {
		settings := fmt.Sprintf(`{"paths": {%q: %q}}`, member.FriendlyPath, member.RootFolder)
		mock.ExpectQuery("FROM hosts WHERE name = \\$1").
			WithArgs(member.HostName).
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
				AddRow(i+1, member.HostName, member.Hostname, "", "", []byte(settings), time.Now()))
	}
}

// expectGroupDedupeDuplicates mocks the duplicate candidate query and the
// follow-up query that loads every location of that hash.
func expectGroupDedupeDuplicates(mock sqlmock.Sqlmock, members []scenarioGroupMember, hash string, size int64, copies []scenarioFileCopy) {
	scopeArgs := make([]driver.Value, 0, len(members)*2)
	for _, member := range members {
		scopeArgs = append(scopeArgs, member.Hostname, member.RootFolder)
	}
	mock.ExpectQuery("HAVING COUNT\\(\\*\\) > 1").
		WithArgs(scopeArgs...).
		WillReturnRows(sqlmock.NewRows([]string{"hash", "size", "count", "total_size"}).
			AddRow(hash, size, len(copies), size*int64(len(copies))))

	locationRows := sqlmock.NewRows([]string{"hash", "path", "hostname", "root_folder", "size"})
	for _, file := range copies {
		locationRows.AddRow(hash, file.Path, file.Hostname, file.RootFolder, size)
	}
	mock.ExpectQuery("WHERE f.hash = \\$1").
		WithArgs(hash, size).
		WillReturnRows(locationRows)
}

func captureStdout(t *testing.T, run func()) string {
	t.Helper()
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writer
	run()
	_ = writer.Close()
	os.Stdout = original
	defer reader.Close()

	var output bytes.Buffer
	if _, err := io.Copy(&output, reader); err != nil {
		t.Fatalf("read captured output: %v", err)
	}
	return output.String()
}

func requireContains(t *testing.T, output string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Fatalf("output is missing %q:\n%s", want, output)
		}
	}
}

func familyScenarioMembers() []scenarioGroupMember {
	return []scenarioGroupMember{
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", RootFolder: "/brain/personal", Priority: 100},
		{HostName: "PI4", Hostname: "pi4", FriendlyPath: "BKP_Media", RootFolder: "/pi4/bkp", Priority: 100},
		{HostName: "Pinky", Hostname: "pinky", FriendlyPath: "Personal", RootFolder: "/pinky/personal", Priority: 100},
	}
}

// A file with copies on only two of the group's three hosts is copied to the
// third host, keeping one copy per host and removing every other copy.
func TestDedupeGroupPlansOneCopyPerHostAcrossThreeHosts(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	const (
		hash = "79f2c7fdd4672fdfd5b3c581daa64800c84351bf95c3e4dbb866c47730c0ce5a"
		size = int64(29951662080)
	)
	members := familyScenarioMembers()

	expectGroupDedupeSetup(mock, "family", 3, members)
	expectGroupDedupeDuplicates(mock, members, hash, size, []scenarioFileCopy{
		{Path: "PINKY/brain2/i00025.avi", Hostname: "brain", RootFolder: "/brain/personal"},
		{Path: "i/precover3/i00025.avi", Hostname: "brain", RootFolder: "/brain/personal"},
		{Path: "pinky2/recover/i00025.avi", Hostname: "brain", RootFolder: "/brain/personal"},
		{Path: "pinky2/recover/i00025.avi", Hostname: "pi4", RootFolder: "/pi4/bkp"},
	})

	var runErr error
	output := captureStdout(t, func() {
		runErr = DeduplicateByGroup(context.Background(), database, GroupDedupeOptions{
			GroupName:   "family",
			BalanceMode: "priority",
			DryRun:      true,
		})
	})
	if runErr != nil {
		t.Fatalf("DeduplicateByGroup: %v", runErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}

	requireContains(t, output,
		"Target: 3 copies, one per host across 3 hosts",
		"Keeping 2 copies:",
		"- Brain:Personal/PINKY/brain2/i00025.avi",
		"- PI4:BKP_Media/pinky2/recover/i00025.avi",
		"Would create 1 missing copies:",
		"- Brain:Personal/PINKY/brain2/i00025.avi -> Pinky:Personal/PINKY/brain2/i00025.avi",
		"Would remove 2 copies:",
		"- Brain:Personal/i/precover3/i00025.avi",
		"- Brain:Personal/pinky2/recover/i00025.avi",
	)
	if strings.Contains(output, "Would remove 2 copies:\n  - PI4") {
		t.Fatalf("the only PI4 copy must be kept:\n%s", output)
	}
}

// Copies stacked on a single host are trimmed to one, with the rest of the group
// filled in from that copy.
func TestDedupeGroupReplicatesWhenEveryCopyIsOnOneHost(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	const (
		hash = "7a7767089feb1ed99551fae29836ab68a54d0d00b89e3237101117d9da745cad"
		size = int64(18500311040)
	)
	members := familyScenarioMembers()

	expectGroupDedupeSetup(mock, "family", 3, members)
	expectGroupDedupeDuplicates(mock, members, hash, size, []scenarioFileCopy{
		{Path: "archive/i00026.mov", Hostname: "brain", RootFolder: "/brain/personal"},
		{Path: "zfs_fast/i00026.mov", Hostname: "brain", RootFolder: "/brain/personal"},
	})

	var runErr error
	output := captureStdout(t, func() {
		runErr = DeduplicateByGroup(context.Background(), database, GroupDedupeOptions{
			GroupName:   "family",
			BalanceMode: "priority",
			DryRun:      true,
		})
	})
	if runErr != nil {
		t.Fatalf("DeduplicateByGroup: %v", runErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}

	requireContains(t, output,
		"Keeping 1 copies:",
		"- Brain:Personal/archive/i00026.mov",
		"Would create 2 missing copies:",
		"-> PI4:BKP_Media/archive/i00026.mov",
		"-> Pinky:Personal/archive/i00026.mov",
		"Would remove 1 copies:",
		"- Brain:Personal/zfs_fast/i00026.mov",
		"Dry run: Would create 2 missing copies, remove 1 files",
	)
}
