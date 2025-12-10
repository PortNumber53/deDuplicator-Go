# Version Updates and Messaging

```gherkin
Feature: Version update messaging

  Scenario: Publishing rejects invalid semantic versions
    Given RabbitMQ connection succeeds
    When I run `deduplicator queue version --version not-a-semver`
    Then the command fails before publishing with an invalid version error

  Scenario: Listener shuts down on newer version but ignores older
    Given a running `deduplicator listen` process with version 1.3.5
    When a message with version 1.4.0 arrives
    Then the process acknowledges the message and signals shutdown
    And when a subsequent message with version 1.2.0 arrives
    Then it is acknowledged but ignored without shutdown
```
