package cmd

import (
	"context"
	"database/sql"

	"deduplicator/files"
)

// Command represents a subcommand with its description and usage
type Command struct {
	Name        string
	Description string
	Usage       string
	Help        string   // Detailed help text
	Examples    []string // Example usages
}

// Available commands
var Commands = []Command{
	{
		Name:        "migrate",
		Description: "Run database migrations",
		Usage:       "migrate [up|down|reset|status]",
		Help: `Manage database migrations for schema changes.

Subcommands:
  up     - Apply all pending migrations
  down   - Roll back the last applied migration
  reset  - Drop all tables and reapply migrations
  status - Show current migration status

The migrations are applied in order based on the numeric prefix of the migration files.`,
		Examples: []string{
			"deduplicator migrate up",
			"deduplicator migrate down",
			"deduplicator migrate reset",
			"deduplicator migrate status",
		},
	},
	{
		Name:        "createdb",
		Description: "Initialize or recreate the database schema (deprecated, use migrate instead)",
		Usage:       "createdb [--force]",
		Help: `Initialize or recreate the database schema.

Options:
  --force  Force recreation of tables by dropping existing ones

Note: This command is deprecated. Please use 'migrate up' instead.`,
		Examples: []string{
			"deduplicator createdb",
			"deduplicator createdb --force",
		},
	},
	{
		Name:        "manage",
		Description: "Manage backup hosts (add/edit/delete/list)",
		Usage:       "manage [add|edit|delete|list] [options]",
		Help: `Manage backup hosts in the system.

Subcommands:
  list           - List all registered hosts
  add            - Add a new host
  edit           - Edit an existing host
  delete         - Remove a host

Arguments for add/edit:
  <n>         - Unique identifier for the host
  <hostname>     - DNS hostname or IP address
  <ip>           - IP address (optional)
  <root_path>    - Base directory for file scanning`,
		Examples: []string{
			"deduplicator manage list",
			"deduplicator manage add myhost example.com 192.168.1.100 /data",
			"deduplicator manage edit myhost newhost.com 192.168.1.101 /backup",
			"deduplicator manage delete myhost",
		},
	},
	{
		Name:        "update",
		Description: "Process file paths from stdin and update the database",
		Usage:       "update < file_list.txt",
		Help: `Update the database with file paths from standard input.

Each line from stdin should contain a single file path. The paths will be
associated with the current host and stored in the database for deduplication.`,
		Examples: []string{
			"find /data -type f | deduplicator update",
			"cat file_list.txt | deduplicator update",
		},
	},
	{
		Name:        "hash",
		Description: "Calculate and update file hashes in the database",
		Usage:       "hash [--force] [--renew] [--retry-problematic] [--count N]",
		Help: `Calculate and store file hashes for deduplication.

Options:
  --force              Rehash files even if they already have a hash
  --renew              Recalculate hashes older than 1 week
  --retry-problematic  Retry files that previously timed out
  --count N            Process only N files (0 = unlimited)

Files are hashed using SHA256 for reliable duplicate detection.`,
		Examples: []string{
			"deduplicator hash",
			"deduplicator hash --force",
			"deduplicator hash --retry-problematic",
			"deduplicator hash --count 1000",
		},
	},
	{
		Name:        "prune",
		Description: "Remove entries for files that no longer exist",
		Usage:       "prune",
		Help: `Remove database entries for files that no longer exist on disk.

This command helps keep the database in sync with the actual filesystem.`,
		Examples: []string{
			"deduplicator prune",
		},
	},
	{
		Name:        "organize",
		Description: "Organize duplicate files by moving them",
		Usage:       "organize [--run] [--move DIR] [--strip-prefix PREFIX]",
		Help: `Organize duplicate files by moving them to a new location.

Options:
  --run               Actually move files (default is dry-run)
  --move DIR          Move duplicates to this directory
  --strip-prefix PREFIX  Remove prefix from paths when moving

By default, this runs in dry-run mode and only shows what would be done.`,
		Examples: []string{
			"deduplicator organize --move /backup/dupes",
			"deduplicator organize --run",
			"deduplicator organize --strip-prefix /data",
		},
	},
	{
		Name:        "listen",
		Description: "Listen for version update messages from RabbitMQ",
		Usage:       "listen",
		Help: `Listen for version update messages from RabbitMQ.

This command connects to RabbitMQ and waits for version update notifications.
When a new version is published, the process will exit gracefully.

Requires RabbitMQ environment variables to be set.`,
		Examples: []string{
			"deduplicator listen",
		},
	},
	{
		Name:        "queue version",
		Description: "Publish a version update message to notify running instances",
		Usage:       "queue version [--version VERSION]",
		Help:        `Publish a version update message to notify running instances.`,
		Examples: []string{
			"deduplicator queue version",
			"deduplicator queue version --version 1.2.0",
		},
	},
	{
		Name:        "files",
		Description: "File-related commands (find, list-dupes, move-dupes)",
		Usage:       "files [find|list-dupes|move-dupes] [options]",
		Help: `File-related commands for finding and managing files.

Subcommands:
  find       - Find files for a specific host
  list-dupes - List duplicate files and optionally move them to a destination directory
  move-dupes - Move duplicate files to a target directory

Options for list-dupes:
  --count N           Limit output to N duplicate groups (0 = unlimited)
  --min-size SIZE     Minimum file size to consider (e.g., "1M", "1.5G", "500K")
  --dest DIR          Directory to move duplicates to (if specified)
  --run               Actually move files (default is dry-run)
  --strip-prefix PREFIX  Remove prefix from paths when moving
  --ignore-dest       Ignore files already in destination (default: true)

When moving files, the command will:
  - Keep the duplicate file that is in the folder with the highest number of unique files
  - Move all other duplicate copies to the destination folder while preserving the folder structure

Examples:
  deduplicator files find
  deduplicator files list-dupes --count 10
  deduplicator files list-dupes --min-size 1G
  deduplicator files list-dupes --dest /backup/dupes --run`,
		Examples: []string{
			"deduplicator files find",
			"deduplicator files list-dupes --count 10",
			"deduplicator files list-dupes --min-size 1G",
			"deduplicator files list-dupes --dest /backup/dupes --run",
		},
	},
}

// FindCommand finds a command by name
func FindCommand(name string) *Command {
	for _, cmd := range Commands {
		if cmd.Name == name {
			return &cmd
		}
	}
	return nil
}

func HandlePrune(ctx context.Context, db *sql.DB) error {
	opts := files.PruneOptions{}
	return files.PruneNonExistentFiles(ctx, db, opts)
}
