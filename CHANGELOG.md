# Changelog

## [Unreleased]

### Added
- **Path Grouping Feature**: Group paths across multiple hosts for load-balanced deduplication
  - New database tables: `path_groups` and `path_group_members`
  - Management commands: `group-add`, `group-list`, `group-delete`, `group-add-path`, `group-remove-path`, `group-show`
  - New deduplication command: `files dedupe-group` for cross-host duplicate management
  - Priority-based file retention strategy (lower priority = higher preference to keep)
  - Respects min/max copy limits per group
  - Supports dry-run mode for safe testing
  - Migration: `000005_add_path_groups.up.sql`

### Fixed
- Fixed Jenkins deployment failure on ARM64 hosts (rpi4)
  - Changed scp destination path from `/tmp/deduplicator` to `/tmp/deduplicator-binary`
  - Updated install command to reference correct binary path
  - Issue: scp was creating a directory instead of copying a file, causing `install: omitting directory` error
  - Affected: `scripts/deploy.sh` lines 37 and 55
