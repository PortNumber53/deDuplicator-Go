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
	"time"
)

// GroupDedupeOptions extends DedupeOptions with group-aware settings
type GroupDedupeOptions struct {
	GroupName     string // Path group to process
	BalanceMode   string // "priority", "equal", "capacity"
	RespectLimits bool   // Honor min/max copies from group settings
	DryRun        bool   // If true, only show what would be done
	MinSize       int64  // Minimum file size to consider
	Count         int    // Limit the number of duplicate groups to process
	Verbose       bool   // Show detailed progress for group resolution and queries
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
	MemberIndex  int
}

// groupDedupeReplication is a copy that must be created on a group host that is
// missing the file before excess copies elsewhere can be removed.
type groupDedupeReplication struct {
	Source    FileLocation
	SrcMember groupMember
	DstMember groupMember
	RelPath   string
}

// groupDedupePlan is the keep/copy/remove decision for a single hash.
type groupDedupePlan struct {
	Keep      []FileLocation
	Replicate []groupDedupeReplication
	Remove    []FileLocation
}

// DeduplicateByGroup performs group-aware deduplication across multiple hosts
func DeduplicateByGroup(ctx context.Context, database *sql.DB, opts GroupDedupeOptions) error {
	// Get path group configuration
	group, err := db.GetPathGroup(database, opts.GroupName)
	if err != nil {
		return fmt.Errorf("error getting path group: %v", err)
	}

	// Get all members of the group, resolved to concrete hosts and root folders
	members, err := resolveGroupMembers(database, opts.GroupName)
	if err != nil {
		return err
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

	hostOrder, _ := groupDedupeHostTargets(members)
	targetCopies := groupDedupeTargetCopies(effectiveGroup, len(hostOrder))

	fmt.Printf("Processing path group '%s' (min_copies=%d, max_copies=%s)\n",
		effectiveGroup.Name, effectiveGroup.MinCopies, formatMaxCopies(effectiveGroup.MaxCopies))
	fmt.Printf("Group members: %d paths across %d hosts\n", len(members), len(hostOrder))
	fmt.Printf("Target: %d copies, one per host across %d hosts\n\n", targetCopies, min(targetCopies, len(hostOrder)))
	groupDedupeVerbosef(opts, "Stored limits: min_copies=%d, max_copies=%s; respect_limits=%t",
		group.MinCopies, formatMaxCopies(group.MaxCopies), opts.RespectLimits)
	groupDedupeVerbosef(opts, "Mode=%s, balance_mode=%s, min_size=%s bytes, count_limit=%s",
		groupDedupeMode(opts), opts.BalanceMode, formatBytes(opts.MinSize), formatGroupDedupeCountLimit(opts.Count))
	for i, member := range members {
		groupDedupeVerbosef(opts, "Member %d/%d: %s:%s -> %s (priority=%d)",
			i+1, len(members), member.HostName, member.FriendlyPath, member.RootFolder, member.Priority)
	}

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
	totalCopied := 0
	totalSaved := int64(0)
	failedGroups := 0

	for _, dupGroup := range duplicates {
		copied, removed, saved, err := processGroupDuplicates(ctx, database, dupGroup, members, targetCopies, opts)
		totalCopied += copied
		totalRemoved += removed
		totalSaved += saved
		if err != nil {
			logging.ErrorLogger.Printf("Error processing hash %s: %v", dupGroup[0].Hash, err)
			failedGroups++
			continue
		}
	}

	if opts.DryRun {
		fmt.Printf("\nDry run: Would create %d missing copies, remove %d files, saving %s\n", totalCopied, totalRemoved, formatBytes(totalSaved))
	} else {
		fmt.Printf("\nCreated %d missing copies, removed %d files, saved %s\n", totalCopied, totalRemoved, formatBytes(totalSaved))
	}
	if failedGroups > 0 {
		return fmt.Errorf("failed to process %d duplicate file groups; see error log for details", failedGroups)
	}

	return nil
}

// groupDedupeTargetCopies returns how many copies of each file the run keeps.
// Host diversity comes first: the group keeps one copy per distinct member host,
// raised to min_copies when the group has fewer hosts than that, and capped by
// max_copies when stored limits are honored.
func groupDedupeTargetCopies(group *db.PathGroup, hostCount int) int {
	target := max(hostCount, group.MinCopies)
	if group.MaxCopies != nil {
		target = min(target, *group.MaxCopies)
	}
	return max(target, 1)
}

// groupDedupeHostTargets lists the distinct hosts of a group in preference
// order, along with the preferred member path to use on each host.
func groupDedupeHostTargets(members []groupMember) ([]string, map[string]groupMember) {
	order := make([]string, 0, len(members))
	best := make(map[string]groupMember, len(members))
	for _, member := range members {
		key := strings.ToLower(member.Hostname)
		if _, ok := best[key]; ok {
			continue
		}
		best[key] = member
		order = append(order, key)
	}
	return order, best
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

func groupDedupeVerbosef(opts GroupDedupeOptions, format string, args ...interface{}) {
	if opts.Verbose {
		fmt.Printf("VERBOSE: "+format+"\n", args...)
	}
}

func groupDedupeMode(opts GroupDedupeOptions) string {
	if opts.DryRun {
		return "dry-run"
	}
	return "run"
}

func formatGroupDedupeCountLimit(count int) string {
	if count <= 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%d", count)
}

// findGroupDuplicates finds all duplicate files across hosts in a path group
func findGroupDuplicates(ctx context.Context, database *sql.DB, members []groupMember, opts GroupDedupeOptions) ([][]FileLocation, error) {
	// Build query to find files across all group members
	query := `
		WITH group_files AS (
			SELECT f.hash, f.size
			FROM files f
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
		groupDedupeVerbosef(opts, "Scoping member path %d/%d: %s:%s -> %s",
			i+1, len(members), member.HostName, member.FriendlyPath, member.RootFolder)
		query += fmt.Sprintf("(LOWER(f.hostname) = LOWER($%d) AND f.root_folder = $%d)", argCount+1, argCount+2)
		args = append(args, member.Hostname, member.RootFolder)
		argCount += 2
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

	groupDedupeVerbosef(opts, "Executing duplicate candidate query across %d member paths", len(members))
	queryStarted := time.Now()
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
	groupDedupeVerbosef(opts, "Duplicate candidate query completed in %s: %d candidate groups",
		time.Since(queryStarted).Round(time.Millisecond), len(duplicates))

	// Now get all file locations for each duplicate hash/size pair.
	var result [][]FileLocation
	for i, duplicate := range duplicates {
		groupDedupeVerbosef(opts, "Loading locations for candidate %d/%d: hash=%s size=%s bytes",
			i+1, len(duplicates), duplicate.hash, formatBytes(duplicate.size))
		locations, err := getFileLocationsForHash(ctx, database, duplicate.hash, duplicate.size, members, opts)
		if err != nil {
			return nil, err
		}
		groupDedupeVerbosef(opts, "Candidate %d/%d has %d matching locations in the group",
			i+1, len(duplicates), len(locations))
		if len(locations) > 1 {
			result = append(result, locations)
		}
	}

	return result, nil
}

// getFileLocationsForHash gets all file locations for a specific hash and size within the group.
func getFileLocationsForHash(ctx context.Context, database *sql.DB, hash string, size int64, members []groupMember, opts GroupDedupeOptions) ([]FileLocation, error) {
	// Map each member's host+root folder back to the member that owns it.
	memberByScope := make(map[string]groupMember, len(members))
	for _, member := range members {
		memberByScope[groupDedupeScopeKey(member.Hostname, member.RootFolder)] = member
	}

	query := `
		SELECT f.hash, f.path, f.hostname, f.root_folder, f.size
		FROM files f
		WHERE f.hash = $1
		AND f.size = $2
		ORDER BY f.hostname, f.path
	`

	groupDedupeVerbosef(opts, "Executing location query for hash=%s size=%s bytes", hash, formatBytes(size))
	queryStarted := time.Now()
	rows, err := database.QueryContext(ctx, query, hash, size)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []FileLocation
	for rows.Next() {
		var loc FileLocation
		if err := rows.Scan(&loc.Hash, &loc.Path, &loc.Hostname, &loc.RootFolder, &loc.Size); err != nil {
			return nil, err
		}

		if member, ok := memberByScope[groupDedupeScopeKey(loc.Hostname, loc.RootFolder)]; ok {
			loc.HostName = member.HostName
			loc.FriendlyPath = member.FriendlyPath
			loc.Priority = member.Priority
			loc.MemberIndex = member.Index
			locations = append(locations, loc)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	groupDedupeVerbosef(opts, "Location query completed in %s", time.Since(queryStarted).Round(time.Millisecond))
	return locations, nil
}

// groupDedupeScopeKey identifies one member path (a host plus its root folder).
func groupDedupeScopeKey(hostname, rootFolder string) string {
	return strings.ToLower(hostname) + "\x00" + rootFolder
}

// processGroupDuplicates processes a group of duplicate files: it keeps one copy
// per group host, creates the copies that are missing on other group hosts, and
// removes everything above that.
func processGroupDuplicates(ctx context.Context, database *sql.DB, locations []FileLocation, members []groupMember, targetCopies int, opts GroupDedupeOptions) (int, int, int64, error) {
	if len(locations) == 0 {
		return 0, 0, 0, nil
	}

	fmt.Printf("Hash: %s (size: %s, copies: %d)\n", locations[0].Hash, formatBytes(locations[0].Size), len(locations))

	// Prefer one copy per host; only keep a second copy on a host when the group
	// cannot supply enough distinct hosts. Priority decides which hosts and which
	// path on each host wins ties.
	plan := planGroupDuplicateLocations(locations, members, targetCopies)
	toKeep := plan.Keep
	toRemove := plan.Remove

	// Display what we're keeping
	fmt.Printf("  Keeping %d copies:\n", len(toKeep))
	for _, loc := range toKeep {
		fmt.Printf("  - %s:%s/%s (priority %d)\n", loc.HostName, loc.FriendlyPath, loc.Path, loc.Priority)
	}

	if !opts.DryRun {
		if err := verifyGroupKeepers(ctx, toKeep); err != nil {
			return 0, 0, 0, err
		}
	}

	// Fill in the hosts that do not have this file yet. Removals are held back
	// until every missing copy exists, so a failed transfer never reduces the
	// number of copies below the copies already on disk.
	copied := 0
	if len(plan.Replicate) > 0 {
		if opts.DryRun {
			fmt.Printf("  Would create %d missing copies:\n", len(plan.Replicate))
			for _, task := range plan.Replicate {
				fmt.Printf("  - %s:%s/%s -> %s:%s/%s\n",
					task.Source.HostName, task.Source.FriendlyPath, task.Source.Path,
					task.DstMember.HostName, task.DstMember.FriendlyPath, task.RelPath)
			}
			copied = len(plan.Replicate)
		} else {
			fmt.Printf("  Creating %d missing copies:\n", len(plan.Replicate))
			for _, task := range plan.Replicate {
				fmt.Printf("  - %s:%s/%s -> %s:%s/%s\n",
					task.Source.HostName, task.Source.FriendlyPath, task.Source.Path,
					task.DstMember.HostName, task.DstMember.FriendlyPath, task.RelPath)
				created, err := replicateGroupCopy(ctx, database, task)
				if err != nil {
					return copied, 0, 0, fmt.Errorf("keeping all copies because %s:%s/%s could not be created: %v",
						task.DstMember.HostName, task.DstMember.FriendlyPath, task.RelPath, err)
				}
				copied++
				toKeep = append(toKeep, created)
				if err := verifyGroupKeepers(ctx, []FileLocation{created}); err != nil {
					return copied, 0, 0, err
				}
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
		return copied, removed, saved, fmt.Errorf("failed to remove %d duplicate copies", failed)
	}
	return copied, removed, saved, nil
}

// planGroupDuplicateLocations keeps exactly one copy per group host, schedules a
// copy for every group host that is still missing the file, and removes the rest.
// A second copy on a host is only ever kept when the group has fewer hosts than
// targetCopies, and those extra copies spread over unused member paths first.
// Priority decides which hosts are covered when targetCopies is lower than the
// number of hosts, and which path on a host is kept.
func planGroupDuplicateLocations(locations []FileLocation, members []groupMember, targetCopies int) groupDedupePlan {
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

	var plan groupDedupePlan
	if len(ordered) == 0 {
		return plan
	}
	if targetCopies <= 0 {
		plan.Remove = ordered
		return plan
	}

	hostOrder, hostMembers := groupDedupeHostTargets(members)
	// Cover the preferred hosts first when the target is lower than the host count.
	coverage := hostOrder
	if targetCopies < len(coverage) {
		coverage = coverage[:targetCopies]
	}
	covering := make(map[string]bool, len(coverage))
	for _, hostKey := range coverage {
		covering[hostKey] = true
	}

	selected := make([]bool, len(ordered))
	covered := make(map[string]bool, len(coverage))
	sourceIndex := -1
	for i, loc := range ordered {
		hostKey := strings.ToLower(loc.Hostname)
		if !covering[hostKey] || covered[hostKey] {
			continue
		}
		selected[i] = true
		covered[hostKey] = true
		if sourceIndex < 0 {
			sourceIndex = i
		}
	}

	if sourceIndex < 0 {
		// No copy lives on a covered host. Keep the most preferred copy rather
		// than deleting the file outright, and give up one coverage slot to it.
		selected[0] = true
		covered[strings.ToLower(ordered[0].Hostname)] = true
		sourceIndex = 0
		if len(coverage) > 0 {
			coverage = coverage[:len(coverage)-1]
		}
	}

	source := ordered[sourceIndex]
	if source.MemberIndex >= 0 && source.MemberIndex < len(members) {
		for _, hostKey := range coverage {
			if covered[hostKey] {
				continue
			}
			plan.Replicate = append(plan.Replicate, groupDedupeReplication{
				Source:    source,
				SrcMember: members[source.MemberIndex],
				DstMember: hostMembers[hostKey],
				RelPath:   source.Path,
			})
		}
	}

	// Only when the group cannot supply enough distinct hosts do additional
	// copies on an already-covered host count toward the target. Spread those
	// extra copies over member paths that are not in use yet before doubling up
	// inside a single member path.
	extra := targetCopies - len(covered) - len(plan.Replicate)
	usedMembers := make(map[int]bool, len(members))
	for i, loc := range ordered {
		if selected[i] {
			usedMembers[loc.MemberIndex] = true
		}
	}
	for _, freshMemberOnly := range []bool{true, false} {
		for i, loc := range ordered {
			if extra <= 0 {
				break
			}
			if selected[i] || !covering[strings.ToLower(loc.Hostname)] {
				continue
			}
			if freshMemberOnly && usedMembers[loc.MemberIndex] {
				continue
			}
			selected[i] = true
			usedMembers[loc.MemberIndex] = true
			extra--
		}
	}

	for i, loc := range ordered {
		if selected[i] {
			plan.Keep = append(plan.Keep, loc)
		} else {
			plan.Remove = append(plan.Remove, loc)
		}
	}
	return plan
}

// replicateGroupCopy creates one missing copy on a group host and indexes it,
// reusing the mirror-group transfer path.
func replicateGroupCopy(ctx context.Context, database *sql.DB, task groupDedupeReplication) (FileLocation, error) {
	localHost, err := os.Hostname()
	if err != nil {
		return FileLocation{}, err
	}
	localHost = strings.ToLower(localHost)

	mirrorTask := groupMirrorTask{
		Hash:      task.Source.Hash,
		Size:      task.Source.Size,
		RelPath:   task.RelPath,
		SrcMember: task.SrcMember,
		DstMember: task.DstMember,
	}

	conflictRoot, conflictHash, conflicts, err := groupMirrorIndexedPathConflict(ctx, database, mirrorTask)
	if err != nil {
		return FileLocation{}, err
	}
	if conflicts {
		return FileLocation{}, fmt.Errorf("destination path is already indexed under root_folder %s with hash %s", conflictRoot, conflictHash)
	}

	dstAbs := filepath.Join(task.DstMember.RootFolder, task.RelPath)
	exists, err := groupMirrorFileExists(ctx, localHost, task.DstMember, dstAbs)
	if err != nil {
		return FileLocation{}, err
	}
	if exists {
		return FileLocation{}, fmt.Errorf("destination file exists on disk but is not indexed with this hash")
	}

	if err := ensureGroupMirrorParentDir(ctx, localHost, task.DstMember, dstAbs); err != nil {
		return FileLocation{}, err
	}
	if err := copyGroupMirrorFile(ctx, localHost, mirrorTask); err != nil {
		return FileLocation{}, err
	}
	if err := recordGroupMirrorCopy(ctx, database, mirrorTask); err != nil {
		return FileLocation{}, err
	}

	return FileLocation{
		Hash:         task.Source.Hash,
		Path:         task.RelPath,
		Hostname:     task.DstMember.Hostname,
		HostName:     task.DstMember.HostName,
		FriendlyPath: task.DstMember.FriendlyPath,
		RootFolder:   task.DstMember.RootFolder,
		Size:         task.Source.Size,
		Priority:     task.DstMember.Priority,
		MemberIndex:  task.DstMember.Index,
	}, nil
}

// verifyGroupKeepers refuses to continue unless every copy that must survive is
// still on disk with its recorded size.
func verifyGroupKeepers(ctx context.Context, keepers []FileLocation) error {
	for _, loc := range keepers {
		matches, err := groupFileMatchesRecordedSize(ctx, loc)
		if err != nil {
			return fmt.Errorf("could not verify keeper %s:%s/%s: %v", loc.HostName, loc.FriendlyPath, loc.Path, err)
		}
		if !matches {
			return fmt.Errorf("refusing to remove duplicates because keeper is missing or its size changed: %s:%s/%s", loc.HostName, loc.FriendlyPath, loc.Path)
		}
	}
	return nil
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
