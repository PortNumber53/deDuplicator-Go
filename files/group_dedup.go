package files

import (
	"context"
	"database/sql"
	"deduplicator/db"
	"deduplicator/logging"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

	fmt.Printf("Processing path group '%s' (min_copies=%d, max_copies=%s)\n",
		group.Name, group.MinCopies, formatMaxCopies(group.MaxCopies))
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

	for _, dupGroup := range duplicates {
		removed, saved, err := processGroupDuplicates(ctx, database, dupGroup, group, members, opts)
		if err != nil {
			logging.ErrorLogger.Printf("Error processing hash %s: %v", dupGroup[0].Hash, err)
			continue
		}
		totalRemoved += removed
		totalSaved += saved
	}

	if opts.DryRun {
		fmt.Printf("\nDry run: Would remove %d files, saving %s\n", totalRemoved, formatBytes(totalSaved))
	} else {
		fmt.Printf("\nRemoved %d files, saved %s\n", totalRemoved, formatBytes(totalSaved))
	}

	return nil
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
		SELECT hash, COUNT(*) as count, SUM(size) as total_size
		FROM group_files
		GROUP BY hash
		HAVING COUNT(*) > 1
		ORDER BY total_size DESC, hash
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

	var hashes []string
	for rows.Next() {
		var hash string
		var count int
		var totalSize int64
		if err := rows.Scan(&hash, &count, &totalSize); err != nil {
			return nil, err
		}
		hashes = append(hashes, hash)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Now get all file locations for each hash
	var result [][]FileLocation
	for _, hash := range hashes {
		locations, err := getFileLocationsForHash(ctx, database, hash, members)
		if err != nil {
			return nil, err
		}
		if len(locations) > 1 {
			result = append(result, locations)
		}
	}

	return result, nil
}

// getFileLocationsForHash gets all file locations for a specific hash within the group
func getFileLocationsForHash(ctx context.Context, database *sql.DB, hash string, members []db.PathGroupMember) ([]FileLocation, error) {
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
		ORDER BY f.hostname, f.path
	`

	rows, err := database.QueryContext(ctx, query, hash)
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

	// Sort by priority (lower = higher priority to keep)
	sort.Slice(locations, func(i, j int) bool {
		if locations[i].Priority != locations[j].Priority {
			return locations[i].Priority < locations[j].Priority
		}
		return locations[i].HostName < locations[j].HostName
	})

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

	// Keep the first keepCount files (highest priority)
	toKeep := locations[:keepCount]
	toRemove := locations[keepCount:]

	// Display what we're keeping
	fmt.Printf("  Keeping %d copies:\n", len(toKeep))
	for _, loc := range toKeep {
		fmt.Printf("  - %s:%s/%s (priority %d)\n", loc.HostName, loc.FriendlyPath, loc.Path, loc.Priority)
	}

	// Display and process removals
	removed := 0
	saved := int64(0)

	if len(toRemove) > 0 {
		if opts.DryRun {
			fmt.Printf("  Would remove %d copies:\n", len(toRemove))
		} else {
			fmt.Printf("  Removing %d copies:\n", len(toRemove))
		}

		for _, loc := range toRemove {
			fullPath := filepath.Join(loc.RootFolder, loc.Path)
			fmt.Printf("  - %s:%s/%s (priority %d)\n", loc.HostName, loc.FriendlyPath, loc.Path, loc.Priority)

			if !opts.DryRun {
				// Delete the file
				if err := os.Remove(fullPath); err != nil {
					if !os.IsNotExist(err) {
						logging.ErrorLogger.Printf("Warning: Failed to delete file %s: %v", fullPath, err)
						continue
					}
				}

				// Remove from database
				_, err := database.Exec(`
					DELETE FROM files
					WHERE path = $1 AND LOWER(hostname) = LOWER($2)
				`, loc.Path, loc.Hostname)
				if err != nil {
					logging.ErrorLogger.Printf("Warning: Failed to delete file from database: %v", err)
					continue
				}
			}

			removed++
			saved += loc.Size
		}
	}

	fmt.Println()
	return removed, saved, nil
}

// formatMaxCopies formats the max copies value
func formatMaxCopies(maxCopies *int) string {
	if maxCopies == nil {
		return "unlimited"
	}
	return fmt.Sprintf("%d", *maxCopies)
}
