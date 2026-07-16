# Pruning and Environment Limits

```gherkin
Feature: Pruning and environment limits

  Scenario: Prune removes missing, symlink, and device entries in batches
    Given files rows include missing files, symlinks, and device entries for the current host
    When I run `deduplicator files prune --batch-size 2`
    Then those rows are deleted in batches of 2 per transaction and progress is shown

  Scenario: Prune honors ENVIRONMENT=local row limiting
    Given ENVIRONMENT is set to "local" and more than 1000 files exist
    When I run `deduplicator files prune`
    Then the query applies a LIMIT between 1000 and 1099 rows for a quick iteration and reports based on the limited set

  Scenario: Prune trusts each row's root_folder
    Given file rows include duplicate entries that resolve to the same root_folder plus path target
    And file rows include stale relative entries with no root_folder
    When I run `deduplicator files prune`
    Then duplicate resolved-path rows and rows without a usable root_folder are deleted

  Scenario: Prune cancellation stops mid-run
    Given prune is running
    When I cancel the context (Ctrl+C)
    Then processing stops, committed batches remain, and the command reports how many files were checked before cancellation
```
