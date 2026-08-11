package files

import (
	"context"
	"database/sql"
	"deduplicator/db"
	"deduplicator/logging"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// GroupDedupeOptions extends DedupeOptions with group-aware settings
type GroupDedupeOptions struct {
	GroupName     string // Path group to process
	BalanceMode   string // "priority", "equal", "capacity"
	RespectLimits bool   // Honor min/max copies from group settings
	DryRun        bool   // If true, only show what would be done
	MinSize       int64  // Minimum file size to consider
	Count         int    // Limit the number of duplicate groups to process
}

// FileLocation represents a file's location with metadata
type FileLocation struct {
	Hash         string
	Path         string
	Hostname     string
	HostName     string
	FriendlyPath string
	RootFolder   string
	Size         int64
	Priority     int
}

// DeduplicateByGroup performs group-aware deduplication across multiple hosts
func DeduplicateByGroup(ctx context.Context, database *sql.DB, opts GroupDedupeOptions) error {
	// Get path group configuration
	group, err := db.GetPathGroup(database, opts.GroupName)
	if err != nil {
		return fmt.Errorf("error getting path group: %v", err)
	}

	// Get all members of the group
	members, err := db.ListGroupMembers(database, opts.GroupName)
	if err != nil {
		return fmt.Errorf("error listing group members: %v", err)
	}

	if len(members) == 0 {
		return fmt.Errorf("path group '%s' has no members", opts.GroupName)
	}

	effectiveGroup := effectiveGroupCopyLimits(group, len(members), opts.RespectLimits)
	if effectiveGroup.MinCopies < 1 {
		return fmt.Errorf("path group '%s' has invalid min_copies=%d: must be at least 1", effectiveGroup.Name, effectiveGroup.MinCopies)
	}
	if effectiveGroup.MaxCopies != nil && *effectiveGroup.MaxCopies < effectiveGroup.MinCopies {
		return fmt.Errorf("path group '%s' has invalid copy limits: max_copies=%d is less than min_copies=%d", effectiveGroup.Name, *effectiveGroup.MaxCopies, effectiveGroup.MinCopies)
	}

	fmt.Printf("Processing path group '%s' (min_copies=%d, max_copies=%s)\n",
		effectiveGroup.Name, effectiveGroup.MinCopies, formatMaxCopies(effectiveGroup.MaxCopies))
	fmt.Printf("Group members: %d paths across hosts\n\n", len(members))

	// Find duplicates across all hosts in the group
	duplicates, err := findGroupDuplicates(ctx, database, members, opts)
	if err != nil {
		return fmt.Errorf("error finding duplicates: %v", err)
	}

	if len(duplicates) == 0 {
		fmt.Println("No duplicates found in this group.")
		return nil
	}

	fmt.Printf("Found %d duplicate file groups\n\n", len(duplicates))

	// Process each duplicate group
	totalRemoved := 0
	totalSaved := int64(0)
	failedGroups := 0

	for _, dupGroup := range duplicates {
		removed, saved, err := processGroupDuplicates(ctx, database, dupGroup, effectiveGroup, members, opts)
		totalRemoved += removed
		totalSaved += saved
		if err != nil {
			logging.ErrorLogger.Printf("Error processing hash %s: %v", dupGroup[0].Hash, err)
			failedGroups++
			continue
		}
	}

	if opts.DryRun {
		fmt.Printf("\nDry run: Would remove %d files, saving %s\n", totalRemoved, formatBytes(totalSaved))
	} else {
		fmt.Printf("\nRemoved %d files, saved %s\n", totalRemoved, formatBytes(totalSaved))
	}
	if failedGroups > 0 {
		return fmt.Errorf("failed to process %d duplicate file groups; see error log for details", failedGroups)
	}

	return nil
}

// effectiveGroupCopyLimits returns the copy limits used for this run. By
// default, group deduplication retains as many copies as there are member
// paths. Stored group limits are an explicit opt-in through --respect-limits.
func effectiveGroupCopyLimits(group *db.PathGroup, memberCount int, respectLimits bool) *db.PathGroup {
	effective := *group
	if !respectLimits {
		effective.MinCopies = memberCount
		effective.MaxCopies = nil
	}
	return &effective
}

// findGroupDuplicates finds all duplicate files across hosts in a path group
func findGroupDuplicates(ctx context.Context, database *sql.DB, members []db.PathGroupMember, opts GroupDedupeOptions) ([][]FileLocation, error) {
	// Build query to find files across all group members
	query := `
		WITH group_files AS (
			SELECT f.hash, f.path, f.hostname, f.root_folder, f.size, h.name as host_name
			FROM files f
			JOIN hosts h ON LOWER(f.hostname) = LOWER(h.hostname)
			WHERE f.hash IS NOT NULL
			AND f.hash NOT IN ('TIMEOUT_ERROR', 'HASH_ERROR')
			AND f.size IS NOT NULL
			AND (
	`

	args := []interface{}{}
	argCount := 0

	// Add conditions for each group member
	for i, member := range members {
		if i > 0 {
			query += " OR "
		}
		argCount++
		query += fmt.Sprintf("(h.name = $%d", argCount)
		args = append(args, member.HostName)

		// Get the absolute path for this friendly path
		host, err := db.GetHost(database, member.HostName)
		if err != nil {
			return nil, err
		}
		paths, err := host.GetPaths()
		if err != nil {
			return nil, err
		}
		absPath, ok := paths[member.FriendlyPath]
		if !ok {
			return nil, fmt.Errorf("friendly path '%s' not found on host '%s'", member.FriendlyPath, member.HostName)
		}

		argCount++
		query += fmt.Sprintf(" AND f.root_folder = $%d)", argCount)
		args = append(args, absPath)
	}

	query += ")"

	if opts.MinSize > 0 {
		argCount++
		query += fmt.Sprintf(" AND f.size >= $%d", argCount)
		args = append(args, opts.MinSize)
	}

	query += `
		)
		SELECT hash, size, COUNT(*) as count, SUM(size) as total_size
		FROM group_files
		GROUP BY hash, size
		HAVING COUNT(*) > 1
		ORDER BY total_size DESC, hash, size
	`

	if opts.Count > 0 {
		argCount++
		query += fmt.Sprintf(" LIMIT $%d", argCount)
		args = append(args, opts.Count)
	}

	rows, err := database.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error querying duplicates: %v", err)
	}
	defer rows.Close()

	type duplicateKey struct {
		hash string
		size int64
	}
	var duplicates []duplicateKey
	for rows.Next() {
		var hash string
		var size int64
		var count int
		var totalSize int64
		if err := rows.Scan(&hash, &size, &count, &totalSize); err != nil {
			return nil, err
		}
		duplicates = append(duplicates, duplicateKey{hash: hash, size: size})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Now get all file locations for each duplicate hash/size pair.
	var result [][]FileLocation
	for _, duplicate := range duplicates {
		locations, err := getFileLocationsForHash(ctx, database, duplicate.hash, duplicate.size, members)
		if err != nil {
			return nil, err
		}
		if len(locations) > 1 {
			result = append(result, locations)
		}
	}

	return result, nil
}

// getFileLocationsForHash gets all file locations for a specific hash and size within the group.
func getFileLocationsForHash(ctx context.Context, database *sql.DB, hash string, size int64, members []db.PathGroupMember) ([]FileLocation, error) {
	// Create a map of host+path to priority
	priorityMap := make(map[string]int)
	friendlyPathMap := make(map[string]string)
	for _, member := range members {
		host, err := db.GetHost(database, member.HostName)
		if err != nil {
			continue
		}
		paths, err := host.GetPaths()
		if err != nil {
			continue
		}
		absPath, ok := paths[member.FriendlyPath]
		if ok {
			key := fmt.Sprintf("%s:%s", member.HostName, absPath)
			priorityMap[key] = member.Priority
			friendlyPathMap[key] = member.FriendlyPath
		}
	}

	query := `
		SELECT f.hash, f.path, f.hostname, f.root_folder, f.size, h.name
		FROM files f
		JOIN hosts h ON LOWER(f.hostname) = LOWER(h.hostname)
		WHERE f.hash = $1
		AND f.size = $2
		ORDER BY f.hostname, f.path
	`

	rows, err := database.QueryContext(ctx, query, hash, size)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []FileLocation
	for rows.Next() {
		var loc FileLocation
		if err := rows.Scan(&loc.Hash, &loc.Path, &loc.Hostname, &loc.RootFolder, &loc.Size, &loc.HostName); err != nil {
			return nil, err
		}

		key := fmt.Sprintf("%s:%s", loc.HostName, loc.RootFolder)
		if priority, ok := priorityMap[key]; ok {
			loc.Priority = priority
			loc.FriendlyPath = friendlyPathMap[key]
			locations = append(locations, loc)
		}
	}

	return locations, rows.Err()
}

// processGroupDuplicates processes a group of duplicate files and decides which to keep/remove
func processGroupDuplicates(ctx context.Context, database *sql.DB, locations []FileLocation, group *db.PathGroup, members []db.PathGroupMember, opts GroupDedupeOptions) (int, int64, error) {
	if len(locations) < 2 {
		return 0, 0, nil
	}

	fmt.Printf("Hash: %s (size: %s, copies: %d)\n", locations[0].Hash, formatBytes(locations[0].Size), len(locations))

	// Determine how many copies to keep
	keepCount := group.MinCopies
	if opts.RespectLimits && group.MaxCopies != nil && len(locations) > *group.MaxCopies {
		keepCount = *group.MaxCopies
	} else if len(locations) <= group.MinCopies {
		// Already at or below minimum, don't remove any
		fmt.Printf("  Keeping all %d copies (at or below minimum)\n", len(locations))
		for _, loc := range locations {
			fmt.Printf("  - %s:%s/%s (priority %d)\n", loc.HostName, loc.FriendlyPath, loc.Path, loc.Priority)
		}
		fmt.Println()
		return 0, 0, nil
	}

	// Prefer one copy per host before retaining additional copies on a host.
	// Priority determines which hosts and which path on each host win ties.
	toKeep, toRemove := planGroupDuplicateLocations(locations, keepCount)

	// Display what we're keeping
	fmt.Printf("  Keeping %d copies:\n", len(toKeep))
	for _, loc := range toKeep {
		fmt.Printf("  - %s:%s/%s (priority %d)\n", loc.HostName, loc.FriendlyPath, loc.Path, loc.Priority)
	}

	if !opts.DryRun {
		for _, loc := range toKeep {
			matches, err := groupFileMatchesRecordedSize(ctx, loc)
			if err != nil {
				return 0, 0, fmt.Errorf("could not verify keeper %s:%s/%s: %v", loc.HostName, loc.FriendlyPath, loc.Path, err)
			}
			if !matches {
				return 0, 0, fmt.Errorf("refusing to remove duplicates because keeper is missing or its size changed: %s:%s/%s", loc.HostName, loc.FriendlyPath, loc.Path)
			}
		}
	}

	// Display and process removals
	removed := 0
	saved := int64(0)
	failed := 0

	if len(toRemove) > 0 {
		if opts.DryRun {
			fmt.Printf("  Would remove %d copies:\n", len(toRemove))
		} else {
			fmt.Printf("  Removing %d copies:\n", len(toRemove))
		}

		for _, loc := range toRemove {
			fmt.Printf("  - %s:%s/%s (priority %d)\n", loc.HostName, loc.FriendlyPath, loc.Path, loc.Priority)

			if !opts.DryRun {
				if err := removeGroupFile(ctx, loc); err != nil {
					logging.ErrorLogger.Printf("Warning: Failed to delete %s:%s/%s: %v", loc.HostName, loc.FriendlyPath, loc.Path, err)
					failed++
					continue
				}

				// Remove from database
				result, err := database.Exec(`
					DELETE FROM files
					WHERE path = $1 AND LOWER(hostname) = LOWER($2)
					AND root_folder = $3 AND hash = $4 AND size = $5
				`, loc.Path, loc.Hostname, loc.RootFolder, loc.Hash, loc.Size)
				if err != nil {
					logging.ErrorLogger.Printf("Warning: Failed to delete file from database: %v", err)
					failed++
					continue
				}
				rows, err := result.RowsAffected()
				if err != nil || rows != 1 {
					logging.ErrorLogger.Printf("Warning: Removed file but expected one matching database row, got rows=%d error=%v", rows, err)
					failed++
					continue
				}
			}

			removed++
			saved += loc.Size
		}
	}

	fmt.Println()
	if failed > 0 {
		return removed, saved, fmt.Errorf("failed to remove %d duplicate copies", failed)
	}
	return removed, saved, nil
}

// planGroupDuplicateLocations chooses keepers by priority while maximizing host diversity.
// Only after every available host has contributed a keeper may another copy from a host be kept.
func planGroupDuplicateLocations(locations []FileLocation, keepCount int) ([]FileLocation, []FileLocation) {
	ordered := append([]FileLocation(nil), locations...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Priority != ordered[j].Priority {
			return ordered[i].Priority < ordered[j].Priority
		}
		if !strings.EqualFold(ordered[i].HostName, ordered[j].HostName) {
			return strings.ToLower(ordered[i].HostName) < strings.ToLower(ordered[j].HostName)
		}
		if ordered[i].FriendlyPath != ordered[j].FriendlyPath {
			return ordered[i].FriendlyPath < ordered[j].FriendlyPath
		}
		if ordered[i].Path != ordered[j].Path {
			return ordered[i].Path < ordered[j].Path
		}
		return ordered[i].RootFolder < ordered[j].RootFolder
	})

	if keepCount >= len(ordered) {
		return ordered, nil
	}
	if keepCount <= 0 {
		return nil, ordered
	}

	selected := make([]bool, len(ordered))
	selectedHosts := make(map[string]bool)
	selectedCount := 0

	for i, loc := range ordered {
		hostKey := strings.ToLower(loc.HostName)
		if selectedHosts[hostKey] {
			continue
		}
		selected[i] = true
		selectedHosts[hostKey] = true
		selectedCount++
		if selectedCount == keepCount {
			break
		}
	}

	for i := range ordered {
		if selectedCount == keepCount {
			break
		}
		if selected[i] {
			continue
		}
		selected[i] = true
		selectedCount++
	}

	toKeep := make([]FileLocation, 0, keepCount)
	toRemove := make([]FileLocation, 0, len(ordered)-keepCount)
	for i, loc := range ordered {
		if selected[i] {
			toKeep = append(toKeep, loc)
		} else {
			toRemove = append(toRemove, loc)
		}
	}
	return toKeep, toRemove
}

func groupFileMatchesRecordedSize(ctx context.Context, loc FileLocation) (bool, error) {
	fullPath := filepath.Join(loc.RootFolder, loc.Path)
	localHost, err := os.Hostname()
	if err != nil {
		return false, err
	}
	if strings.EqualFold(localHost, loc.Hostname) {
		info, err := os.Stat(fullPath)
		if err == nil {
			return info.Mode().IsRegular() && info.Size() == loc.Size, nil
		}
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	quotedPath := shellEscape(fullPath)
	command := fmt.Sprintf("test -f %s && test \"$(stat -c %%s -- %s)\" -eq %d", quotedPath, quotedPath, loc.Size)
	cmd := exec.CommandContext(ctx, "ssh", loc.Hostname, command)
	err = cmd.Run()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("remote file check failed: %v", err)
}

func removeGroupFile(ctx context.Context, loc FileLocation) error {
	fullPath := filepath.Join(loc.RootFolder, loc.Path)
	localHost, err := os.Hostname()
	if err != nil {
		return err
	}
	if strings.EqualFold(localHost, loc.Hostname) {
		info, err := os.Stat(fullPath)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() != loc.Size {
			return fmt.Errorf("file is missing or its size changed")
		}
		if err := os.Remove(fullPath); err != nil {
			return err
		}
		return nil
	}

	quotedPath := shellEscape(fullPath)
	command := fmt.Sprintf("test -f %s && test \"$(stat -c %%s -- %s)\" -eq %d && rm -- %s", quotedPath, quotedPath, loc.Size, quotedPath)
	cmd := exec.CommandContext(ctx, "ssh", loc.Hostname, command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("remote remove failed: %v %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

// formatMaxCopies formats the max copies value
func formatMaxCopies(maxCopies *int) string {
	if maxCopies == nil {
		return "unlimited"
	}
	return fmt.Sprintf("%d", *maxCopies)
}
