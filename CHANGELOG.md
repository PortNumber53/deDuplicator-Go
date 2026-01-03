# Changelog

## [Unreleased]

### Fixed
- Fixed Jenkins deployment failure on ARM64 hosts (rpi4)
  - Changed scp destination path from `/tmp/deduplicator` to `/tmp/deduplicator-binary`
  - Updated install command to reference correct binary path
  - Issue: scp was creating a directory instead of copying a file, causing `install: omitting directory` error
  - Affected: `scripts/deploy.sh` lines 37 and 55
