# Ingest and Hashing

```gherkin
Feature: File ingest and hashing

  Scenario: Processing stdin inserts only regular files
    Given the OS hostname matches a host row
    When I pipe a mix of files, directories, symlinks, and device files to `deduplicator update`
    Then only regular files are upserted with their sizes and directories or special files are skipped with warnings

  Scenario: Finding files records root_folder for each friendly path
    Given host "Backup1" has a friendly path "photos" mapped to "/data/photos"
    When I run `deduplicator files find --server Backup1 --path photos`
    Then every regular file under /data/photos is stored with path relative to /data/photos and root_folder set to "/data/photos"

  Scenario: Hashing only unhashed duplicate-size files by default
    Given files rows for host "backup1.local" with some NULL hashes and repeated file sizes
    When I run `deduplicator files hash`
    Then only rows with NULL hash and a size shared by another file are processed, their SHA256 is stored, and last_hashed_at is updated

  Scenario: Hashing larger files first
    Given files rows for host "backup1.local" with some NULL hashes and repeated file sizes
    When I run `deduplicator files hash --large-first`
    Then rows with NULL hash and a size shared by another file are processed from largest size to smallest size using full-file SHA256 hashes

  Scenario: Prioritizing friendly paths while hashing
    Given host "Backup1" has friendly paths "photos" and "videos" mapped to root folders
    And files rows for host "backup1.local" with some NULL hashes and repeated file sizes across multiple root folders
    When I run `deduplicator files hash --path photos --path videos`
    Then eligible files under "photos" are processed first, eligible files under "videos" are processed next, and all other eligible files are processed after them

  Scenario: Hashing with compatible combined flags
    Given files rows for host "backup1.local" with some NULL hashes and repeated file sizes
    When I run `deduplicator files hash --full-hash --large-first`
    Then all rows with NULL hash are processed from largest size to smallest size using full-file SHA256 hashes

  Scenario: Full hashing includes unique-size files
    Given files rows for host "backup1.local" with some NULL hashes
    When I run `deduplicator files hash --full-hash`
    Then every row with NULL hash is processed regardless of file size uniqueness

  Scenario: Upgrading stored hashes to full hashes
    Given files rows for host "backup1.local" with stored non-error hashes
    When I run `deduplicator files hash-upgrade`
    Then each stored hash is compared to a newly calculated full-file SHA256 hash
    And rows whose stored hash differs are updated to the full-file hash

  Scenario: Retrying problematic hashes marks TIMEOUT_ERROR and retries them
    Given a file that timed out and is marked TIMEOUT_ERROR
    When I run `deduplicator files hash --retry-problematic`
    Then the file is re-attempted and either gets a new hash or is re-marked TIMEOUT_ERROR

  Scenario: Force hashing recalculates existing hashes
    Given files with existing hashes
    When I run `deduplicator files hash --force`
    Then hashes are recomputed regardless of last_hashed_at and existing values

  Scenario: Hashing fails cleanly when host is unknown
    Given the OS hostname is not present in hosts
    When I run `deduplicator files hash`
    Then the command errors with guidance to add the host
```
