# Importing and Mirroring

```gherkin
Feature: Importing and mirroring files

  Scenario: Import fails when friendly path mapping is missing
    Given host "Backup1" lacks a mapping for friendly path "photos"
    When I run `deduplicator files import --source /staging --server Backup1 --path photos`
    Then the command warns about the missing mapping, falls back to host.root_path/photos, and proceeds only if the directory exists

  Scenario: Import skips existing targets and hashes new files
    Given /staging contains files that also exist at the destination
    When I run `deduplicator files import --source /staging --server Backup1 --path photos`
    Then existing target files are skipped, new files are rsynced, hashed locally, and inserted or updated in files with the host's canonical hostname

  Scenario: Import with duplicate directory relocates target conflicts
    Given /staging/file1 also exists at the destination and --duplicate /dupes is provided
    When I run the import without --dry-run
    Then the conflicting source file is moved to /dupes/file1 and counted in the move summary

  Scenario: Import with age and remove-source rules
    Given /staging has files newer and older than 10 minutes
    When I run `deduplicator files import --age 10 --remove-source`
    Then files newer than 10 minutes are skipped, older files are transferred, and only successfully transferred files are removed from source

  Scenario: Mirror friendly path copies missing files and reports conflicts
    Given at least two hosts share friendly path "photos" with identical hashes for some files and differing hashes for others
    When I run `deduplicator files mirror photos`
    Then files missing on a host are rsynced from a source host, while hash mismatches or on-disk-but-not-in-DB cases are reported as conflicts
```
