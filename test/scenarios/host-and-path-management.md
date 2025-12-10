# Host and Path Management

```gherkin
Feature: Host and path management

  Scenario: Adding a host requires a hostname
    Given the hosts table is empty
    When I run `deduplicator manage server-add "Backup1" --hostname backup1.local --ip 10.0.0.5`
    Then a host row named "Backup1" with lowercased hostname "backup1.local" is stored and a duplicate name or hostname is rejected

  Scenario: Editing a host preserves unspecified fields
    Given a host "Backup1" with hostname "backup1.local" and ip "10.0.0.5"
    When I run `deduplicator manage server-edit "Backup1" --hostname backup1.lan`
    Then the hostname is updated to "backup1.lan" while the friendly name and ip remain unchanged

  Scenario: Listing servers shows guidance when empty
    Given no hosts exist
    When I run `deduplicator manage server-list`
    Then the command prints "No servers found. Use 'deduplicator manage server-add' to add a server."

  Scenario: Adding and editing friendly paths updates JSON settings
    Given host "Backup1" has no paths
    When I run `deduplicator manage path-add "Backup1" "photos" "/data/photos"`
    And I run `deduplicator manage path-edit "Backup1" "photos" "/mnt/photos"`
    Then the settings JSON stores "photos": "/mnt/photos" and `path-list` shows the updated absolute path

  Scenario: Deleting a missing friendly path is reported but not fatal
    Given host "Backup1" has paths that do not include "docs"
    When I run `deduplicator manage path-delete "Backup1" "docs"`
    Then the command reports the path is not found and leaves settings unchanged
```
