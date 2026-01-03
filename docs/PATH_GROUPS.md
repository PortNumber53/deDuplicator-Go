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
  --respect-limits       Honor min/max copy limits from group settings
  --dry-run              Show what would be done without making changes (default)
  --run                  Actually perform the deduplication
  --min-size <bytes>     Only process files larger than this size
  --count <n>            Limit the number of duplicate groups to process
```

## How It Works

### Priority-Based Retention

When deduplicating a group, the system:

1. **Finds duplicates** across all hosts in the group
2. **Sorts by priority** (lower = keep first)
3. **Keeps minimum copies** from highest priority hosts
4. **Removes excess copies** from lower priority hosts

### Example Scenario

Given:
- Group "photos" with min_copies=2, max_copies=3
- brain (priority 10), pinky (priority 50), rpi4 (priority 100)
- File "vacation.jpg" exists on all three hosts

Result:
- **Keep**: brain (priority 10), pinky (priority 50)
- **Remove**: rpi4 (priority 100)

### Respecting Limits

With `--respect-limits`:
- If copies < min_copies: Keep all copies (don't remove any)
- If copies > max_copies: Remove excess from lowest priority hosts
- If min_copies ≤ copies ≤ max_copies: Remove excess beyond min_copies

Without `--respect-limits`:
- Always keep exactly min_copies (from highest priority hosts)

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
