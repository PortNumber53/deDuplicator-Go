# Path Grouping Feature

## Overview

Path grouping allows you to associate paths across multiple hosts for intelligent, load-balanced deduplication. Instead of managing duplicates on a single host, you can define groups of related paths and maintain a balanced number of copies across your infrastructure.

## Use Cases

- **Backup Distribution**: Keep 2-3 copies of important files distributed across different backup servers
- **Load Balancing**: Distribute file storage across multiple hosts based on priority
- **Redundancy Management**: Ensure minimum copies while preventing excessive duplication

## Quick Start

### 1. Create a Path Group

```bash
deduplicator manage group-add photos \
  --min-copies 2 \
  --max-copies 3 \
  --description "Family photos distributed across backup servers"
```

### 2. Add Paths to the Group

```bash
# Add paths from different hosts (lower priority = higher preference to keep)
deduplicator manage group-add-path photos brain photos --priority 10
deduplicator manage group-add-path photos pinky photos --priority 50
deduplicator manage group-add-path photos rpi4 photos --priority 100
```

### 3. View Group Configuration

```bash
deduplicator manage group-show photos
```

### 4. Run Group Deduplication

```bash
# Dry run first to see what would happen
deduplicator files dedupe-group photos --dry-run

# Actually perform the deduplication
deduplicator files dedupe-group photos --run
```

### 5. Mirror Missing Group Copies

```bash
# Show missing copies without transferring files
deduplicator files mirror-group photos --dry-run

# Copy missing hashes to every path in the group
deduplicator files mirror-group photos
```

## Management Commands

### Create a Group
```bash
deduplicator manage group-add <group_name> [options]

Options:
  --min-copies N         Minimum copies to maintain (default: 2)
  --max-copies N         Maximum copies to keep (default: unlimited)
  --description "text"   Description of the group
```

### List All Groups
```bash
deduplicator manage group-list
```

### Show Group Details
```bash
deduplicator manage group-show <group_name>
```

### Delete a Group
```bash
deduplicator manage group-delete <group_name>
```

### Add Path to Group
```bash
deduplicator manage group-add-path <group_name> <host_name> <friendly_path> [--priority N]

Priority:
  - Lower numbers = higher priority to keep files
  - Default: 100
  - Example: Use 10 for primary storage, 50 for secondary, 100 for tertiary
```

### Remove Path from Group
```bash
deduplicator manage group-remove-path <host_name> <friendly_path>
```

## Deduplication Command

```bash
deduplicator files dedupe-group <group_name> [options]

Options:
  --balance-mode <mode>  Balance mode: priority (default), equal, capacity
  --respect-limits       Honor stored limits instead of using the group member count
  --dry-run              Show what would be done without making changes (default)
  --run                  Actually perform the deduplication
  --min-size <bytes>     Only process files larger than this size
  --count <n>            Limit the number of duplicate groups to process
  --verbose              Show member, query, and candidate processing details
```

`dedupe-group` keeps **one copy per host** in the group. For every duplicated
file it:

1. Keeps the highest-priority copy on each group host that has one.
2. Copies the file to group hosts that do not have it yet (same transfer path as
   `mirror-group`).
3. Removes every remaining copy.

New copies are placed at the relative path chosen the same way `mirror-group`
chooses it: the path that already has the most copies of that hash, with ties
resolved in favour of the member with the most indexed files. The copy is
transferred from a copy the run is keeping whenever one sits at that path, so
both commands agree on where a copy belongs instead of propagating whichever
nested path a single host happens to use.

A second copy on the same host is only kept when the group has fewer hosts than
the target copy count. For example, a group whose three member paths live on
Brain, PI4, and Pinky ends up with three copies on three different hosts,
regardless of how many copies started out on a single host.

The target copy count is the number of distinct member hosts, raised to
`min_copies` when the group has fewer hosts than that. By default `min_copies`
is the number of member paths in the group; use `--respect-limits` to apply the
stored `min_copies` and `max_copies` values instead. When `max_copies` is lower
than the number of hosts, only the highest-priority hosts are covered and copies
on the remaining hosts are removed. The last copy of a file is never removed.

The run summary reports both sides of the ledger — the bytes written by new
copies and the bytes reclaimed by removals — along with the net change, which is
negative when filling in missing hosts costs more space than the removals free.

Removals for a file are held back until all of its missing copies exist, so a
failed transfer leaves the existing copies in place. Files that have only one
copy in the group are not touched; use `mirror-group` to populate every member
path for those.

Verbose mode reports each member path as it is resolved, when the duplicate
candidate and location queries begin, how long they take, and how many rows are
selected for processing. This is useful for long-running searches on large
file indexes.

## Mirroring Command

```bash
deduplicator files mirror-group <group_name> [--dry-run]
```

`mirror-group` treats the number of paths in the group as the desired mirror
copy count. It uses full-file hash values as the file identity, so different
relative paths with the same hash are treated as the same file. When it needs
to create a missing copy and existing copies use different relative paths, it
chooses the relative path that already has the most copies for that hash. Ties
are resolved by using the path from the group member with the most indexed
files, reducing unnecessary folder/path proliferation.

## How It Works

### One Copy Per Host

When deduplicating a group, the system:

1. **Finds duplicates** across all hosts in the group
2. **Sorts by priority** (lower = keep first)
3. **Keeps the best copy on each host** it is meant to cover
4. **Copies the file to hosts that do not have it**
5. **Removes every other copy**, including extra copies on a host it already
   keeps a copy on

### Example Scenario

Given:
- Group "family" with members on Brain, PI4, and Pinky (target: 3 copies)
- File "i00025.avi" has 4 copies on Brain and 1 copy on PI4

Result:
- **Keep**: the highest-priority Brain copy, the PI4 copy
- **Copy**: Brain → Pinky (Pinky has no copy yet)
- **Remove**: the 3 extra Brain copies

The file ends up with 3 copies on 3 different hosts.

### Respecting Limits

With `--respect-limits`:
- Target copies = number of member hosts, raised to `min_copies` and capped by
  `max_copies`
- If `max_copies` is lower than the host count, only the highest-priority hosts
  keep a copy

Without `--respect-limits`:
- Target copies = number of member hosts, raised to the number of group members
  when several member paths share a host

## Database Schema

### path_groups Table
```sql
CREATE TABLE path_groups (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    min_copies INT DEFAULT 2,
    max_copies INT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### path_group_members Table
```sql
CREATE TABLE path_group_members (
    id SERIAL PRIMARY KEY,
    group_id INT NOT NULL REFERENCES path_groups(id) ON DELETE CASCADE,
    host_name TEXT NOT NULL REFERENCES hosts(name) ON DELETE CASCADE,
    friendly_path TEXT NOT NULL,
    priority INT DEFAULT 100,
    UNIQUE(group_id, host_name, friendly_path),
    UNIQUE(host_name, friendly_path)
);
```

## Best Practices

1. **Start with dry-run**: Always test with `--dry-run` first
2. **Set appropriate priorities**: Use 10, 50, 100 for clear priority tiers
3. **Monitor min/max copies**: Ensure min_copies matches your redundancy requirements
4. **Group related paths**: Only group paths that contain the same logical data
5. **Test incrementally**: Use `--count` to process a few duplicate groups first

## Troubleshooting

### Path not found error
```
Error: friendly path 'photos' not found on host 'brain'
```
**Solution**: Ensure the path exists on the host using `deduplicator manage path-list <host_name>`

### No duplicates found
```
No duplicates found in this group.
```
**Possible causes**:
- Files haven't been hashed yet (run `deduplicator files hash`)
- Paths don't contain the same files
- Files are already deduplicated

### Group already exists
```
Error: duplicate key value violates unique constraint
```
**Solution**: Use a different group name or delete the existing group first

## Migration

The path grouping feature requires database migration:

```bash
deduplicator migrate up
```

This will create the `path_groups` and `path_group_members` tables.

## Future Enhancements

- **Capacity-based balancing**: Distribute files based on available disk space
- **Equal distribution mode**: Balance file counts evenly across hosts
- **Automatic rebalancing**: Periodic jobs to maintain balance
- **Group templates**: Predefined group configurations for common scenarios
