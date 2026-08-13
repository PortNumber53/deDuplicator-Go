package files

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"deduplicator/db"
)

func TestGroupDedupeVerboseOutputIsOptIn(t *testing.T) {
	capture := func(opts GroupDedupeOptions) string {
		original := os.Stdout
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe: %v", err)
		}
		os.Stdout = writer
		groupDedupeVerbosef(opts, "Executing duplicate candidate query across %d member paths", 3)
		_ = writer.Close()
		os.Stdout = original
		defer reader.Close()

		var output bytes.Buffer
		if _, err := io.Copy(&output, reader); err != nil {
			t.Fatalf("read captured output: %v", err)
		}
		return output.String()
	}

	if output := capture(GroupDedupeOptions{}); output != "" {
		t.Fatalf("non-verbose output = %q, want empty", output)
	}
	output := capture(GroupDedupeOptions{Verbose: true})
	if !strings.Contains(output, "VERBOSE: Executing duplicate candidate query across 3 member paths") {
		t.Fatalf("verbose output = %q", output)
	}
}

func TestEffectiveGroupCopyLimitsDefaultsToMemberCount(t *testing.T) {
	configuredMax := 3
	configured := &db.PathGroup{Name: "family", MinCopies: 2, MaxCopies: &configuredMax}

	effective := effectiveGroupCopyLimits(configured, 3, false)

	if effective.MinCopies != 3 {
		t.Fatalf("min copies = %d, want member count 3", effective.MinCopies)
	}
	if effective.MaxCopies != nil {
		t.Fatalf("max copies = %d, want unlimited when stored limits are not requested", *effective.MaxCopies)
	}
	if configured.MinCopies != 2 || configured.MaxCopies == nil || *configured.MaxCopies != 3 {
		t.Fatalf("configured group was mutated: %+v", configured)
	}
}

func TestEffectiveGroupCopyLimitsUsesStoredLimitsWhenRequested(t *testing.T) {
	configuredMax := 3
	configured := &db.PathGroup{Name: "family", MinCopies: 2, MaxCopies: &configuredMax}

	effective := effectiveGroupCopyLimits(configured, 4, true)

	if effective.MinCopies != 2 {
		t.Fatalf("min copies = %d, want configured value 2", effective.MinCopies)
	}
	if effective.MaxCopies == nil || *effective.MaxCopies != 3 {
		t.Fatalf("max copies = %v, want configured value 3", effective.MaxCopies)
	}
}

// familyGroupMembers mirrors a three-host group: Brain, PI4, and Pinky.
func familyGroupMembers() []groupMember {
	return []groupMember{
		{Index: 0, HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", RootFolder: "/brain/personal", Priority: 100, FileCount: 900},
		{Index: 1, HostName: "PI4", Hostname: "pi4", FriendlyPath: "BKP_Media", RootFolder: "/pi4/bkp", Priority: 100, FileCount: 500},
		{Index: 2, HostName: "Pinky", Hostname: "pinky", FriendlyPath: "Personal", RootFolder: "/pinky/personal", Priority: 100, FileCount: 100},
	}
}

func planHosts(locations []FileLocation) []string {
	hosts := make([]string, 0, len(locations))
	for _, loc := range locations {
		hosts = append(hosts, loc.HostName)
	}
	return hosts
}

func TestPlanGroupDuplicateLocationsKeepsOneCopyPerHost(t *testing.T) {
	members := familyGroupMembers()
	locations := []FileLocation{
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "copy-a", Priority: 100, MemberIndex: 0},
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "copy-b", Priority: 100, MemberIndex: 0},
		{HostName: "PI4", Hostname: "pi4", FriendlyPath: "BKP_Media", Path: "copy-c", Priority: 100, MemberIndex: 1},
		{HostName: "Pinky", Hostname: "pinky", FriendlyPath: "Personal", Path: "copy-d", Priority: 100, MemberIndex: 2},
	}

	plan := planGroupDuplicateLocations(locations, members, 3)

	if len(plan.Keep) != 3 || len(plan.Remove) != 1 || len(plan.Replicate) != 0 {
		t.Fatalf("plan = %d keep, %d remove, %d replicate; want 3, 1, 0",
			len(plan.Keep), len(plan.Remove), len(plan.Replicate))
	}
	if got, want := strings.Join(planHosts(plan.Keep), ","), "Brain,PI4,Pinky"; got != want {
		t.Fatalf("keeper hosts = %s; want %s", got, want)
	}
	if plan.Remove[0].Path != "copy-b" {
		t.Fatalf("removed %s; want the second Brain copy", plan.Remove[0].Path)
	}
}

func TestPlanGroupDuplicateLocationsReplicatesToHostsWithoutACopy(t *testing.T) {
	members := familyGroupMembers()
	locations := []FileLocation{
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "dir/i00025.avi", Priority: 100, MemberIndex: 0},
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "other/i00025.avi", Priority: 100, MemberIndex: 0},
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "third/i00025.avi", Priority: 100, MemberIndex: 0},
		{HostName: "PI4", Hostname: "pi4", FriendlyPath: "BKP_Media", Path: "dir/i00025.avi", Priority: 100, MemberIndex: 1},
	}

	plan := planGroupDuplicateLocations(locations, members, 3)

	if len(plan.Keep) != 2 || len(plan.Remove) != 2 {
		t.Fatalf("plan = %d keep, %d remove; want 2 and 2", len(plan.Keep), len(plan.Remove))
	}
	if got, want := strings.Join(planHosts(plan.Keep), ","), "Brain,PI4"; got != want {
		t.Fatalf("keeper hosts = %s; want %s", got, want)
	}
	if len(plan.Replicate) != 1 {
		t.Fatalf("replication tasks = %d; want 1 copy to the host without one", len(plan.Replicate))
	}
	task := plan.Replicate[0]
	if task.DstMember.HostName != "Pinky" {
		t.Fatalf("replication destination = %s; want Pinky", task.DstMember.HostName)
	}
	if task.SrcMember.HostName != "Brain" || task.Source.Path != "dir/i00025.avi" || task.RelPath != "dir/i00025.avi" {
		t.Fatalf("replication source = %+v, rel path = %s; want the preferred Brain copy", task.Source, task.RelPath)
	}
}

func TestPlanGroupDuplicateLocationsReplicatesWhenAllCopiesShareOneHost(t *testing.T) {
	members := familyGroupMembers()
	locations := []FileLocation{
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "copy-a", Priority: 100, MemberIndex: 0},
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "copy-b", Priority: 100, MemberIndex: 0},
	}

	plan := planGroupDuplicateLocations(locations, members, 3)

	if len(plan.Keep) != 1 || plan.Keep[0].Path != "copy-a" {
		t.Fatalf("keepers = %+v; want only the preferred Brain copy", plan.Keep)
	}
	if len(plan.Remove) != 1 || plan.Remove[0].Path != "copy-b" {
		t.Fatalf("removals = %+v; want the extra Brain copy", plan.Remove)
	}
	if len(plan.Replicate) != 2 {
		t.Fatalf("replication tasks = %d; want copies to PI4 and Pinky", len(plan.Replicate))
	}
	if plan.Replicate[0].DstMember.HostName != "PI4" || plan.Replicate[1].DstMember.HostName != "Pinky" {
		t.Fatalf("replication destinations = %s, %s; want PI4, Pinky",
			plan.Replicate[0].DstMember.HostName, plan.Replicate[1].DstMember.HostName)
	}
}

func TestPlanGroupDuplicateLocationsUsesPriorityAcrossHosts(t *testing.T) {
	members := []groupMember{
		{Index: 0, HostName: "Brain", Hostname: "brain", FriendlyPath: "Preferred", RootFolder: "/brain/preferred", Priority: 10},
		{Index: 1, HostName: "Brain", Hostname: "brain", FriendlyPath: "Secondary", RootFolder: "/brain/secondary", Priority: 50},
		{Index: 2, HostName: "PI4", Hostname: "pi4", FriendlyPath: "BKP_Media", RootFolder: "/pi4/bkp", Priority: 100},
		{Index: 3, HostName: "Pinky", Hostname: "pinky", FriendlyPath: "Personal", RootFolder: "/pinky/personal", Priority: 200},
	}
	locations := []FileLocation{
		{HostName: "Brain", Hostname: "brain", Path: "less-preferred", Priority: 50, MemberIndex: 1},
		{HostName: "Brain", Hostname: "brain", Path: "preferred", Priority: 10, MemberIndex: 0},
		{HostName: "PI4", Hostname: "pi4", Path: "backup", Priority: 100, MemberIndex: 2},
		{HostName: "Pinky", Hostname: "pinky", Path: "archive", Priority: 200, MemberIndex: 3},
	}

	plan := planGroupDuplicateLocations(locations, members, 2)

	if len(plan.Keep) != 2 || len(plan.Replicate) != 0 {
		t.Fatalf("plan = %d keep, %d replicate; want 2 and 0", len(plan.Keep), len(plan.Replicate))
	}
	if plan.Keep[0].HostName != "Brain" || plan.Keep[0].Path != "preferred" {
		t.Fatalf("first keeper = %+v; want preferred Brain copy", plan.Keep[0])
	}
	if plan.Keep[1].HostName != "PI4" {
		t.Fatalf("second keeper host = %s; want PI4", plan.Keep[1].HostName)
	}
	if len(plan.Remove) != 2 {
		t.Fatalf("removals = %+v; want the extra Brain copy and the uncovered Pinky copy", plan.Remove)
	}
}

func TestPlanGroupDuplicateLocationsKeepsSecondCopyOnlyWhenHostsRunOut(t *testing.T) {
	members := []groupMember{
		{Index: 0, HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", RootFolder: "/brain/personal", Priority: 100},
		{Index: 1, HostName: "Brain", Hostname: "brain", FriendlyPath: "Media", RootFolder: "/brain/media", Priority: 100},
		{Index: 2, HostName: "PI4", Hostname: "pi4", FriendlyPath: "BKP_Media", RootFolder: "/pi4/bkp", Priority: 100},
	}
	locations := []FileLocation{
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "copy-a", Priority: 100, MemberIndex: 0},
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Media", Path: "copy-b", Priority: 100, MemberIndex: 1},
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Media", Path: "copy-c", Priority: 100, MemberIndex: 1},
		{HostName: "PI4", Hostname: "pi4", FriendlyPath: "BKP_Media", Path: "copy-d", Priority: 100, MemberIndex: 2},
	}

	// Three member paths across two hosts: the third copy has nowhere else to go.
	plan := planGroupDuplicateLocations(locations, members, 3)

	if len(plan.Keep) != 3 || len(plan.Remove) != 1 || len(plan.Replicate) != 0 {
		t.Fatalf("plan = %d keep, %d remove, %d replicate; want 3, 1, 0",
			len(plan.Keep), len(plan.Remove), len(plan.Replicate))
	}
	if plan.Remove[0].Path != "copy-c" {
		t.Fatalf("removed %s; want the third Brain copy", plan.Remove[0].Path)
	}
}

func TestPlanGroupDuplicateLocationsNeverRemovesTheLastCopy(t *testing.T) {
	members := familyGroupMembers()
	locations := []FileLocation{
		{HostName: "Pinky", Hostname: "pinky", FriendlyPath: "Personal", Path: "copy-a", Priority: 100, MemberIndex: 2},
		{HostName: "Pinky", Hostname: "pinky", FriendlyPath: "Personal", Path: "copy-b", Priority: 100, MemberIndex: 2},
	}

	// A max_copies of 1 covers only Brain, which holds no copy of this file.
	plan := planGroupDuplicateLocations(locations, members, 1)

	if len(plan.Keep) != 1 || plan.Keep[0].Path != "copy-a" {
		t.Fatalf("keepers = %+v; want the single best Pinky copy", plan.Keep)
	}
	if len(plan.Replicate) != 0 {
		t.Fatalf("replication tasks = %d; want none when the target is already met", len(plan.Replicate))
	}
	if len(plan.Remove) != 1 || plan.Remove[0].Path != "copy-b" {
		t.Fatalf("removals = %+v; want the extra Pinky copy", plan.Remove)
	}
}

func TestGroupDedupeTargetCopies(t *testing.T) {
	maxCopies := func(n int) *int { return &n }

	tests := []struct {
		name      string
		group     *db.PathGroup
		hostCount int
		want      int
	}{
		{name: "one copy per host", group: &db.PathGroup{MinCopies: 3}, hostCount: 3, want: 3},
		{name: "min copies above host count", group: &db.PathGroup{MinCopies: 4}, hostCount: 2, want: 4},
		{name: "hosts above min copies", group: &db.PathGroup{MinCopies: 2}, hostCount: 3, want: 3},
		{name: "max copies caps hosts", group: &db.PathGroup{MinCopies: 2, MaxCopies: maxCopies(2)}, hostCount: 3, want: 2},
		{name: "never below one", group: &db.PathGroup{MinCopies: 0}, hostCount: 0, want: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := groupDedupeTargetCopies(test.group, test.hostCount); got != test.want {
				t.Fatalf("target copies = %d, want %d", got, test.want)
			}
		})
	}
}

func TestGroupFileMatchesRecordedSize(t *testing.T) {
	root := t.TempDir()
	path := "video.bin"
	contents := []byte("family backup")
	if err := os.WriteFile(filepath.Join(root, path), contents, 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	localHost, err := os.Hostname()
	if err != nil {
		t.Fatalf("hostname: %v", err)
	}

	loc := FileLocation{Hostname: localHost, RootFolder: root, Path: path, Size: int64(len(contents))}
	matches, err := groupFileMatchesRecordedSize(context.Background(), loc)
	if err != nil || !matches {
		t.Fatalf("matching file: matches=%v err=%v", matches, err)
	}

	loc.Size++
	matches, err = groupFileMatchesRecordedSize(context.Background(), loc)
	if err != nil {
		t.Fatalf("changed-size check: %v", err)
	}
	if matches {
		t.Fatal("file with changed size unexpectedly matched")
	}
}

func TestRemoveGroupFileRefusesChangedSize(t *testing.T) {
	root := t.TempDir()
	path := "video.bin"
	fullPath := filepath.Join(root, path)
	if err := os.WriteFile(fullPath, []byte("family backup"), 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	localHost, err := os.Hostname()
	if err != nil {
		t.Fatalf("hostname: %v", err)
	}

	loc := FileLocation{Hostname: localHost, RootFolder: root, Path: path, Size: 1}
	if err := removeGroupFile(context.Background(), loc); err == nil {
		t.Fatal("expected changed-size removal to fail")
	}
	if _, err := os.Stat(fullPath); err != nil {
		t.Fatalf("file should remain after refused removal: %v", err)
	}
}

// New copies go to the relative path that already has the most copies, the same
// rule mirror-group applies, instead of the keeper's own path.
func TestPlanGroupDuplicateLocationsReplicatesToTheMostCommonPath(t *testing.T) {
	members := familyGroupMembers()
	locations := []FileLocation{
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "PINKY/brain2/deep/file.mov", Priority: 100, MemberIndex: 0},
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "shared/file.mov", Priority: 100, MemberIndex: 0},
		{HostName: "PI4", Hostname: "pi4", FriendlyPath: "BKP_Media", Path: "shared/file.mov", Priority: 100, MemberIndex: 1},
	}

	plan := planGroupDuplicateLocations(locations, members, 3)

	if len(plan.Replicate) != 1 {
		t.Fatalf("replication tasks = %d; want 1", len(plan.Replicate))
	}
	task := plan.Replicate[0]
	if task.RelPath != "shared/file.mov" {
		t.Fatalf("replication path = %s; want the path with the most copies", task.RelPath)
	}
	// The Brain copy at that path is being removed, so the kept PI4 copy is the source.
	if task.SrcMember.HostName != "PI4" || task.Source.Path != "shared/file.mov" {
		t.Fatalf("replication source = %s:%s; want the kept PI4 copy", task.SrcMember.HostName, task.Source.Path)
	}
	if task.DstMember.HostName != "Pinky" {
		t.Fatalf("replication destination = %s; want Pinky", task.DstMember.HostName)
	}
	if len(plan.Keep) != 2 || len(plan.Remove) != 1 || plan.Remove[0].Path != "shared/file.mov" {
		t.Fatalf("plan = %+v; want the deep Brain copy and the PI4 copy kept", plan)
	}
}

// When each candidate path has the same number of copies, the member with the
// most indexed files wins, matching mirror-group's tie-break.
func TestPlanGroupDuplicateLocationsBreaksPathTiesOnIndexedFileCount(t *testing.T) {
	members := familyGroupMembers()
	locations := []FileLocation{
		{HostName: "Brain", Hostname: "brain", FriendlyPath: "Personal", Path: "zeta/file.mov", Priority: 100, MemberIndex: 0},
		{HostName: "PI4", Hostname: "pi4", FriendlyPath: "BKP_Media", Path: "alpha/file.mov", Priority: 100, MemberIndex: 1},
	}

	plan := planGroupDuplicateLocations(locations, members, 3)

	if len(plan.Replicate) != 1 {
		t.Fatalf("replication tasks = %d; want 1", len(plan.Replicate))
	}
	if plan.Replicate[0].RelPath != "zeta/file.mov" {
		t.Fatalf("replication path = %s; want Brain's path, since Brain indexes the most files",
			plan.Replicate[0].RelPath)
	}
}
