# Migrations and Locking

```gherkin
Feature: Database migrations and locking

  Scenario: Applying pending migrations creates migrations records
    Given a clean database with the migrations table present
    When I run `deduplicator migrate up`
    Then each *.up.sql in migrations/ that has not been applied is executed once and recorded in migrations

  Scenario: Rolling back the last migration runs the matching down script
    Given at least one migration has been applied
    When I run `deduplicator migrate down`
    Then the latest applied migration is reversed using its .down.sql and removed from the migrations table

  Scenario: Showing migration status marks missing-on-disk files
    Given the database contains a migration record for a file that no longer exists on disk
    When I run `deduplicator migrate status`
    Then the output lists existing .up.sql files as applied or pending and flags the missing record as "missing in code"

  Scenario: Concurrent migrate commands are serialized by the lock
    Given one migrate process holds `/tmp/deduplicator/migrate.lock`
    When a second migrate command starts
    Then it fails because the lock cannot be acquired until the first process exits or the lock is stale
```
