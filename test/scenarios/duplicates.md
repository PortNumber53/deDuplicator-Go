# Duplicate Detection and Movement

```gherkin
Feature: Duplicate detection and movement

  Scenario: Listing duplicates respects min-size and count filters
    Given files with hashes and sizes on the current host
    When I run `deduplicator files list-dupes --min-size 1048576 --count 2`
    Then only duplicate groups at least 1MB are shown and at most two groups are printed, ordered by total size

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

  Scenario: Move-dupes requires a target and honors dry-run
    Given duplicate groups exist
    When I run `deduplicator files move-dupes --target /tmp/dupes --dry-run`
    Then the command prints planned moves using root_folder plus path for sources and does not modify files or database
```
