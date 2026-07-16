# Duplicate Detection and Movement

```gherkin
Feature: Duplicate detection and movement

  Scenario: Listing duplicates checks across hosts while respecting filters
    Given files with hashes, sizes, and hostnames across multiple hosts
    When I run `deduplicator files list-dupes --min-size 1048576 --count 2`
    Then only cross-host duplicate groups at least 1MB are shown and at most two groups are printed, ordered by total size

  Scenario: Dedup dry-run reports potential moves without touching files
    Given duplicate groups exist and destination directory parent exists
    When I run `deduplicator files list-dupes --dest /tmp/dupes --strip-prefix /data --dry-run`
    Then the command lists which files would be moved with prefix stripped and ends with total potential savings

  Scenario: Dedup run moves all but one file and updates the database
    Given duplicate files in different directories
    When I run `deduplicator files list-dupes --dest /tmp/dupes --run`
    Then for each group all but the file in the most populated directory are moved (using rsync on cross-device errors) and removed from the files table

  Scenario: Dedup ignores files already under the destination when requested
    Given a duplicate set where one copy already resides under /tmp/dupes
    When I run `deduplicator files list-dupes --dest /tmp/dupes --ignore-dest true`
    Then that group is skipped to avoid re-moving files inside the destination tree

  Scenario: Dedup separates partial-hash collisions by size
    Given duplicate candidates have the same hash but different sizes
    When I run `deduplicator files list-dupes --dest /tmp/dupes --dry-run`
    Then files are grouped by both hash and size so dedupe reports valid groups without failing

  Scenario: Move-dupes requires a target and honors dry-run
    Given duplicate groups exist
    When I run `deduplicator files move-dupes --target /tmp/dupes --dry-run`
    Then the command prints planned moves using root_folder plus path for sources and does not modify files or database

  Scenario: Move-dupes archives local files under a host folder
    Given duplicate rows with the same hash and size across hosts "pinky" and "rpi4"
    When I run `deduplicator files move-dupes --target /tmp/dupes --min-size 10G`
    Then only files for the current host are moved locally under /tmp/dupes/<host>/ and remote host files are left for their own host to process
```
