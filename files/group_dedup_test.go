package files

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPlanGroupDuplicateLocationsPrefersDistinctHosts(t *testing.T) {
	locations := []FileLocation{
		{HostName: "Brain", FriendlyPath: "Personal", Path: "copy-a", Priority: 100},
		{HostName: "Brain", FriendlyPath: "Personal", Path: "copy-b", Priority: 100},
		{HostName: "PI4", FriendlyPath: "BKP_Media", Path: "copy-c", Priority: 100},
		{HostName: "Pinky", FriendlyPath: "Personal", Path: "copy-d", Priority: 100},
	}

	keepers, removals := planGroupDuplicateLocations(locations, 2)
	if len(keepers) != 2 || len(removals) != 2 {
		t.Fatalf("got %d keepers and %d removals, want 2 and 2", len(keepers), len(removals))
	}
	if keepers[0].HostName == keepers[1].HostName {
		t.Fatalf("keepers are both on %s; want distinct hosts", keepers[0].HostName)
	}
	if keepers[0].HostName != "Brain" || keepers[1].HostName != "PI4" {
		t.Fatalf("keeper hosts = %s, %s; want Brain, PI4", keepers[0].HostName, keepers[1].HostName)
	}
}

func TestPlanGroupDuplicateLocationsUsesPriorityAcrossHosts(t *testing.T) {
	locations := []FileLocation{
		{HostName: "Brain", Path: "less-preferred", Priority: 50},
		{HostName: "Brain", Path: "preferred", Priority: 10},
		{HostName: "PI4", Path: "backup", Priority: 100},
		{HostName: "Pinky", Path: "archive", Priority: 200},
	}

	keepers, _ := planGroupDuplicateLocations(locations, 2)
	if keepers[0].HostName != "Brain" || keepers[0].Path != "preferred" {
		t.Fatalf("first keeper = %+v; want preferred Brain copy", keepers[0])
	}
	if keepers[1].HostName != "PI4" {
		t.Fatalf("second keeper host = %s; want PI4", keepers[1].HostName)
	}
}

func TestPlanGroupDuplicateLocationsFallsBackToSameHost(t *testing.T) {
	locations := []FileLocation{
		{HostName: "Brain", Path: "copy-c", Priority: 100},
		{HostName: "Brain", Path: "copy-a", Priority: 100},
		{HostName: "Brain", Path: "copy-b", Priority: 100},
	}

	keepers, removals := planGroupDuplicateLocations(locations, 2)
	if len(keepers) != 2 || len(removals) != 1 {
		t.Fatalf("got %d keepers and %d removals, want 2 and 1", len(keepers), len(removals))
	}
	if keepers[0].Path != "copy-a" || keepers[1].Path != "copy-b" {
		t.Fatalf("keeper paths = %s, %s; want deterministic copy-a, copy-b", keepers[0].Path, keepers[1].Path)
	}
}

func TestPlanGroupDuplicateLocationsKeepsAllWhenAtMinimum(t *testing.T) {
	locations := []FileLocation{
		{HostName: "Brain", Path: "copy-a", Priority: 100},
		{HostName: "PI4", Path: "copy-b", Priority: 100},
	}

	keepers, removals := planGroupDuplicateLocations(locations, 2)
	if len(keepers) != 2 || len(removals) != 0 {
		t.Fatalf("got %d keepers and %d removals, want 2 and 0", len(keepers), len(removals))
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
