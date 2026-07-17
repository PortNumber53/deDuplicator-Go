package files

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"deduplicator/db"
	"deduplicator/logging"
)

// GroupMirrorOptions controls group-aware mirroring.
type GroupMirrorOptions struct {
	GroupName string
	DryRun    bool
}

type groupMirrorMember struct {
	Index        int
	HostName     string
	Hostname     string
	FriendlyPath string
	RootFolder   string
	Priority     int
	FileCount    int64
}

type groupMirrorLocation struct {
	Hash        string
	Path        string
	Size        int64
	MemberIndex int
}

type groupMirrorTask struct {
	Hash      string
	Size      int64
	RelPath   string
	Source    groupMirrorLocation
	SrcMember groupMirrorMember
	DstMember groupMirrorMember
}

type groupMirrorConflict struct {
	Hash   string
	Path   string
	Member groupMirrorMember
	Reason string
}

// MirrorGroup mirrors every hash in a path group to every member path.
func MirrorGroup(ctx context.Context, database *sql.DB, opts GroupMirrorOptions) error {
	groupName := strings.TrimSpace(opts.GroupName)
	if groupName == "" {
		return fmt.Errorf("mirror-group requires a group name")
	}

	members, err := resolveGroupMirrorMembers(ctx, database, groupName)
	if err != nil {
		return err
	}
	if len(members) < 2 {
		return fmt.Errorf("path group '%s' needs at least 2 member paths to mirror", groupName)
	}

	hashLocations, memberPathHashes, err := loadGroupMirrorHashes(ctx, database, members)
	if err != nil {
		return err
	}
	if len(hashLocations) == 0 {
		fmt.Printf("No hashed files found in group '%s'.\n", groupName)
		return nil
	}

	tasks, conflicts := planGroupMirrorTasks(hashLocations, members, memberPathHashes)

	fmt.Printf("Mirroring group '%s' across %d paths (target copies per hash: %d)\n", groupName, len(members), len(members))
	fmt.Printf("Found %d unique hashes; %d missing copies to create\n", len(hashLocations), len(tasks))
	if opts.DryRun {
		printGroupMirrorTasks("Would copy", tasks)
		printGroupMirrorConflicts(conflicts)
		return nil
	}

	localHost, _ := os.Hostname()
	localHost = strings.ToLower(localHost)
	copied := 0
	for _, task := range tasks {
		conflictRoot, conflictHash, conflictsWithOtherRoot, err := groupMirrorIndexedPathConflict(ctx, database, task)
		if err != nil {
			conflicts = append(conflicts, groupMirrorConflict{
				Hash:   task.Hash,
				Path:   task.RelPath,
				Member: task.DstMember,
				Reason: err.Error(),
			})
			continue
		}
		if conflictsWithOtherRoot {
			conflicts = append(conflicts, groupMirrorConflict{
				Hash:   task.Hash,
				Path:   task.RelPath,
				Member: task.DstMember,
				Reason: fmt.Sprintf("destination path is already indexed under root_folder %s with hash %s", conflictRoot, conflictHash),
			})
			continue
		}

		dstAbs := filepath.Join(task.DstMember.RootFolder, task.RelPath)
		exists, err := groupMirrorFileExists(ctx, localHost, task.DstMember, dstAbs)
		if err != nil {
			conflicts = append(conflicts, groupMirrorConflict{
				Hash:   task.Hash,
				Path:   task.RelPath,
				Member: task.DstMember,
				Reason: err.Error(),
			})
			continue
		}
		if exists {
			conflicts = append(conflicts, groupMirrorConflict{
				Hash:   task.Hash,
				Path:   task.RelPath,
				Member: task.DstMember,
				Reason: "destination file exists on disk but is not indexed with this hash",
			})
			continue
		}

		if err := ensureGroupMirrorParentDir(ctx, localHost, task.DstMember, dstAbs); err != nil {
			conflicts = append(conflicts, groupMirrorConflict{
				Hash:   task.Hash,
				Path:   task.RelPath,
				Member: task.DstMember,
				Reason: err.Error(),
			})
			continue
		}

		if err := copyGroupMirrorFile(ctx, localHost, task); err != nil {
			conflicts = append(conflicts, groupMirrorConflict{
				Hash:   task.Hash,
				Path:   task.RelPath,
				Member: task.DstMember,
				Reason: err.Error(),
			})
			continue
		}

		if err := recordGroupMirrorCopy(ctx, database, task); err != nil {
			conflicts = append(conflicts, groupMirrorConflict{
				Hash:   task.Hash,
				Path:   task.RelPath,
				Member: task.DstMember,
				Reason: err.Error(),
			})
			continue
		}

		copied++
		fmt.Printf("Copied %s -> %s: %s\n", groupMirrorMemberLabel(task.SrcMember), groupMirrorMemberLabel(task.DstMember), task.RelPath)
	}

	fmt.Printf("\nMirror-group summary: copied %d files", copied)
	if len(conflicts) > 0 {
		fmt.Printf(", %d conflicts/skips", len(conflicts))
	}
	fmt.Println()
	printGroupMirrorConflicts(conflicts)
	return nil
}

func resolveGroupMirrorMembers(ctx context.Context, database *sql.DB, groupName string) ([]groupMirrorMember, error) {
	if _, err := db.GetPathGroup(database, groupName); err != nil {
		return nil, fmt.Errorf("error getting path group: %v", err)
	}

	groupMembers, err := db.ListGroupMembers(database, groupName)
	if err != nil {
		return nil, fmt.Errorf("error listing group members: %v", err)
	}

	members := make([]groupMirrorMember, 0, len(groupMembers))
	for i, member := range groupMembers {
		host, err := db.GetHost(database, member.HostName)
		if err != nil {
			return nil, fmt.Errorf("error getting host '%s': %v", member.HostName, err)
		}
		paths, err := host.GetPaths()
		if err != nil {
			return nil, fmt.Errorf("error decoding paths for host '%s': %v", member.HostName, err)
		}
		rootFolder, ok := paths[member.FriendlyPath]
		if !ok {
			return nil, fmt.Errorf("friendly path '%s' not found on host '%s'", member.FriendlyPath, member.HostName)
		}

		mirrorMember := groupMirrorMember{
			Index:        i,
			HostName:     member.HostName,
			Hostname:     host.Hostname,
			FriendlyPath: member.FriendlyPath,
			RootFolder:   rootFolder,
			Priority:     member.Priority,
		}
		mirrorMember.FileCount, err = countGroupMirrorMemberFiles(ctx, database, mirrorMember)
		if err != nil {
			return nil, err
		}
		members = append(members, mirrorMember)
	}

	return members, nil
}

func countGroupMirrorMemberFiles(ctx context.Context, database *sql.DB, member groupMirrorMember) (int64, error) {
	var count int64
	err := database.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM files
		WHERE LOWER(hostname) = LOWER($1)
		AND root_folder = $2
	`, member.Hostname, member.RootFolder).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error counting files for %s: %v", groupMirrorMemberLabel(member), err)
	}
	return count, nil
}

func loadGroupMirrorHashes(ctx context.Context, database *sql.DB, members []groupMirrorMember) (map[string][]groupMirrorLocation, map[int]map[string]string, error) {
	hashLocations := make(map[string][]groupMirrorLocation)
	memberPathHashes := make(map[int]map[string]string, len(members))

	for _, member := range members {
		memberPathHashes[member.Index] = make(map[string]string)
		rows, err := database.QueryContext(ctx, `
			SELECT path, hash, size
			FROM files
			WHERE LOWER(hostname) = LOWER($1)
			AND root_folder = $2
			AND hash IS NOT NULL
			AND hash NOT IN ('TIMEOUT_ERROR', 'HASH_ERROR')
			AND size IS NOT NULL
			ORDER BY hash, path
		`, member.Hostname, member.RootFolder)
		if err != nil {
			return nil, nil, fmt.Errorf("error loading files for %s: %v", groupMirrorMemberLabel(member), err)
		}

		for rows.Next() {
			var path, hash string
			var size int64
			if err := rows.Scan(&path, &hash, &size); err != nil {
				rows.Close()
				return nil, nil, fmt.Errorf("error scanning files for %s: %v", groupMirrorMemberLabel(member), err)
			}
			memberPathHashes[member.Index][path] = hash
			hashLocations[hash] = append(hashLocations[hash], groupMirrorLocation{
				Hash:        hash,
				Path:        path,
				Size:        size,
				MemberIndex: member.Index,
			})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("error iterating files for %s: %v", groupMirrorMemberLabel(member), err)
		}
		rows.Close()
	}

	return hashLocations, memberPathHashes, nil
}

func planGroupMirrorTasks(hashLocations map[string][]groupMirrorLocation, members []groupMirrorMember, memberPathHashes map[int]map[string]string) ([]groupMirrorTask, []groupMirrorConflict) {
	var tasks []groupMirrorTask
	var conflicts []groupMirrorConflict
	plannedDestPaths := make(map[int]map[string]string, len(members))
	hashes := make([]string, 0, len(hashLocations))
	for hash := range hashLocations {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)

	for _, hash := range hashes {
		locations := hashLocations[hash]
		size, ok := groupMirrorCommonSize(locations)
		if !ok {
			conflicts = append(conflicts, groupMirrorConflict{
				Hash:   hash,
				Reason: "same hash has conflicting sizes",
			})
			continue
		}

		relPath, ok := chooseGroupMirrorPath(locations, members)
		if !ok {
			continue
		}
		cleanRelPath, err := cleanGroupMirrorRelPath(relPath)
		if err != nil {
			conflicts = append(conflicts, groupMirrorConflict{
				Hash:   hash,
				Path:   relPath,
				Reason: err.Error(),
			})
			continue
		}
		source, ok := chooseGroupMirrorSource(locations, members, relPath)
		if !ok {
			continue
		}

		present := make(map[int]struct{}, len(locations))
		for _, loc := range locations {
			present[loc.MemberIndex] = struct{}{}
		}

		for _, member := range members {
			if _, ok := present[member.Index]; ok {
				continue
			}

			if existingHash, ok := memberPathHashes[member.Index][cleanRelPath]; ok && existingHash != hash {
				conflicts = append(conflicts, groupMirrorConflict{
					Hash:   hash,
					Path:   cleanRelPath,
					Member: member,
					Reason: fmt.Sprintf("destination path is already indexed with different hash %s", existingHash),
				})
				continue
			}

			if plannedDestPaths[member.Index] == nil {
				plannedDestPaths[member.Index] = make(map[string]string)
			}
			if plannedHash, ok := plannedDestPaths[member.Index][cleanRelPath]; ok && plannedHash != hash {
				conflicts = append(conflicts, groupMirrorConflict{
					Hash:   hash,
					Path:   cleanRelPath,
					Member: member,
					Reason: fmt.Sprintf("destination path is already planned for different hash %s", plannedHash),
				})
				continue
			}
			plannedDestPaths[member.Index][cleanRelPath] = hash

			tasks = append(tasks, groupMirrorTask{
				Hash:      hash,
				Size:      size,
				RelPath:   cleanRelPath,
				Source:    source,
				SrcMember: members[source.MemberIndex],
				DstMember: member,
			})
		}
	}

	return tasks, conflicts
}

func groupMirrorCommonSize(locations []groupMirrorLocation) (int64, bool) {
	if len(locations) == 0 {
		return 0, false
	}
	size := locations[0].Size
	for _, loc := range locations[1:] {
		if loc.Size != size {
			return 0, false
		}
	}
	return size, true
}

func chooseGroupMirrorPath(locations []groupMirrorLocation, members []groupMirrorMember) (string, bool) {
	if len(locations) == 0 {
		return "", false
	}

	type pathCandidate struct {
		Path                string
		Copies              int
		BestMemberFileCount int64
		BestMemberLabel     string
	}

	candidatesByPath := make(map[string]*pathCandidate)
	for _, loc := range locations {
		member := members[loc.MemberIndex]
		candidate := candidatesByPath[loc.Path]
		if candidate == nil {
			candidate = &pathCandidate{Path: loc.Path, BestMemberLabel: groupMirrorMemberLabel(member)}
			candidatesByPath[loc.Path] = candidate
		}
		candidate.Copies++
		label := groupMirrorMemberLabel(member)
		if member.FileCount > candidate.BestMemberFileCount ||
			(member.FileCount == candidate.BestMemberFileCount && label < candidate.BestMemberLabel) {
			candidate.BestMemberFileCount = member.FileCount
			candidate.BestMemberLabel = label
		}
	}

	candidates := make([]pathCandidate, 0, len(candidatesByPath))
	for _, candidate := range candidatesByPath {
		candidates = append(candidates, *candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Copies != candidates[j].Copies {
			return candidates[i].Copies > candidates[j].Copies
		}
		if candidates[i].BestMemberFileCount != candidates[j].BestMemberFileCount {
			return candidates[i].BestMemberFileCount > candidates[j].BestMemberFileCount
		}
		return candidates[i].Path < candidates[j].Path
	})

	return candidates[0].Path, true
}

func chooseGroupMirrorSource(locations []groupMirrorLocation, members []groupMirrorMember, relPath string) (groupMirrorLocation, bool) {
	var candidates []groupMirrorLocation
	for _, loc := range locations {
		if loc.Path == relPath {
			candidates = append(candidates, loc)
		}
	}
	if len(candidates) == 0 {
		candidates = append(candidates, locations...)
	}
	if len(candidates) == 0 {
		return groupMirrorLocation{}, false
	}
	sort.Slice(candidates, func(i, j int) bool {
		left := members[candidates[i].MemberIndex]
		right := members[candidates[j].MemberIndex]
		if left.FileCount != right.FileCount {
			return left.FileCount > right.FileCount
		}
		return groupMirrorMemberLabel(left) < groupMirrorMemberLabel(right)
	})
	return candidates[0], true
}

func cleanGroupMirrorRelPath(relPath string) (string, error) {
	relPath = strings.TrimSpace(relPath)
	if relPath == "" {
		return "", fmt.Errorf("empty relative path")
	}
	cleaned := filepath.Clean(relPath)
	if cleaned == "." || filepath.IsAbs(cleaned) || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe relative path")
	}
	return cleaned, nil
}

func groupMirrorFileExists(ctx context.Context, localHost string, member groupMirrorMember, absPath string) (bool, error) {
	if groupMirrorIsLocal(localHost, member) {
		_, err := os.Stat(absPath)
		if err == nil {
			return true, nil
		}
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("error checking destination file: %v", err)
	}

	cmd := exec.CommandContext(ctx, "ssh", member.Hostname, "test -e "+shellEscape(absPath))
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("ssh destination check failed: %v", err)
}

func ensureGroupMirrorParentDir(ctx context.Context, localHost string, member groupMirrorMember, absPath string) error {
	parentDir := filepath.Dir(absPath)
	if groupMirrorIsLocal(localHost, member) {
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return fmt.Errorf("mkdir failed: %v", err)
		}
		return nil
	}

	cmd := exec.CommandContext(ctx, "ssh", member.Hostname, "mkdir -p "+shellEscape(parentDir))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("remote mkdir failed: %v %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func copyGroupMirrorFile(ctx context.Context, localHost string, task groupMirrorTask) error {
	srcAbs := filepath.Join(task.SrcMember.RootFolder, task.RelPath)
	dstAbs := filepath.Join(task.DstMember.RootFolder, task.RelPath)

	srcEndpoint := groupMirrorRsyncEndpoint(localHost, task.SrcMember, srcAbs)
	dstEndpoint := groupMirrorRsyncEndpoint(localHost, task.DstMember, dstAbs)
	srcLocal := groupMirrorIsLocal(localHost, task.SrcMember)
	dstLocal := groupMirrorIsLocal(localHost, task.DstMember)

	if srcLocal || dstLocal {
		return runGroupMirrorRsync(ctx, srcEndpoint, dstEndpoint)
	}

	tmpFile, err := os.CreateTemp("", "deduplicator-mirror-*")
	if err != nil {
		return fmt.Errorf("creating temporary transfer file failed: %v", err)
	}
	tmpPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("closing temporary transfer file failed: %v", err)
	}
	defer os.Remove(tmpPath)

	if err := runGroupMirrorRsync(ctx, srcEndpoint, tmpPath); err != nil {
		return err
	}
	return runGroupMirrorRsync(ctx, tmpPath, dstEndpoint)
}

func runGroupMirrorRsync(ctx context.Context, source, destination string) error {
	cmd := exec.CommandContext(ctx, "rsync", "-a", source, destination)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("rsync failed: %v %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func groupMirrorIndexedPathConflict(ctx context.Context, database *sql.DB, task groupMirrorTask) (string, string, bool, error) {
	var rootFolder, hash sql.NullString
	err := database.QueryRowContext(ctx, `
		SELECT root_folder, hash
		FROM files
		WHERE LOWER(hostname) = LOWER($1)
		AND path = $2
		AND COALESCE(root_folder, '') <> $3
		LIMIT 1
	`, task.DstMember.Hostname, task.RelPath, task.DstMember.RootFolder).Scan(&rootFolder, &hash)
	if err == sql.ErrNoRows {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("error checking indexed path conflicts: %v", err)
	}
	return rootFolder.String, hash.String, true, nil
}

func recordGroupMirrorCopy(ctx context.Context, database *sql.DB, task groupMirrorTask) error {
	result, err := database.ExecContext(ctx, `
		INSERT INTO files (path, hostname, size, hash, root_folder, last_hashed_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (path, hostname)
		DO UPDATE SET
			size = EXCLUDED.size,
			hash = EXCLUDED.hash,
			root_folder = EXCLUDED.root_folder,
			last_hashed_at = EXCLUDED.last_hashed_at
		WHERE COALESCE(files.root_folder, '') = COALESCE(EXCLUDED.root_folder, '')
	`, task.RelPath, task.DstMember.Hostname, task.Size, task.Hash, task.DstMember.RootFolder)
	if err != nil {
		return fmt.Errorf("error recording mirrored file: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking mirrored file insert result: %v", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("destination path is already indexed under a different root_folder")
	}
	return nil
}

func groupMirrorRsyncEndpoint(localHost string, member groupMirrorMember, absPath string) string {
	if groupMirrorIsLocal(localHost, member) {
		return absPath
	}
	return member.Hostname + ":" + shellEscape(absPath)
}

func groupMirrorIsLocal(localHost string, member groupMirrorMember) bool {
	return strings.EqualFold(localHost, member.Hostname)
}

func groupMirrorMemberLabel(member groupMirrorMember) string {
	return fmt.Sprintf("%s:%s", member.HostName, member.FriendlyPath)
}

func printGroupMirrorTasks(prefix string, tasks []groupMirrorTask) {
	if len(tasks) == 0 {
		return
	}
	fmt.Printf("\n%s %d files:\n", prefix, len(tasks))
	for _, task := range tasks {
		fmt.Printf("  %s -> %s: %s (%s)\n",
			groupMirrorMemberLabel(task.SrcMember),
			groupMirrorMemberLabel(task.DstMember),
			task.RelPath,
			task.Hash,
		)
	}
}

func printGroupMirrorConflicts(conflicts []groupMirrorConflict) {
	if len(conflicts) == 0 {
		return
	}
	fmt.Printf("\nConflicts/skips:\n")
	for _, conflict := range conflicts {
		member := ""
		if conflict.Member.HostName != "" {
			member = " " + groupMirrorMemberLabel(conflict.Member)
		}
		path := ""
		if conflict.Path != "" {
			path = " " + conflict.Path
		}
		fmt.Printf("  %s%s%s: %s\n", conflict.Hash, member, path, conflict.Reason)
		logging.ErrorLogger.Printf("mirror-group conflict hash=%s member=%s path=%s reason=%s",
			conflict.Hash,
			groupMirrorMemberLabel(conflict.Member),
			conflict.Path,
			conflict.Reason,
		)
	}
}
