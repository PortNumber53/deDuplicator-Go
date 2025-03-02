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
			"dedupe migrate up",
			"dedupe migrate down",
			"dedupe migrate reset",
			"dedupe migrate status",
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
			"dedupe createdb",
			"dedupe createdb --force",
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
			"dedupe manage list",
			"dedupe manage add myhost example.com 192.168.1.100 /data",
			"dedupe manage edit myhost newhost.com 192.168.1.101 /backup",
			"dedupe manage delete myhost",
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
			"find /data -type f | dedupe update",
			"cat file_list.txt | dedupe update",
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
			"dedupe hash",
			"dedupe hash --force",
			"dedupe hash --retry-problematic",
			"dedupe hash --count 1000",
		},
	},
	{
		Name:        "list",
		Description: "List duplicate files (deprecated, use 'files list-dupes' instead)",
		Usage:       "list [--count N] [--min-size SIZE]",
		Help: `List duplicate files in the system.

Options:
  --count N      Limit output to N duplicate groups (0 = unlimited)
  --min-size SIZE  Minimum file size to consider (e.g., "1M", "1.5G", "500K")

Files are considered duplicates if they have the same hash value.
Size units: B (bytes), K/KB, M/MB, G/GB, T/TB (1K = 1024 bytes)

Note: This command is deprecated. Please use 'files list-dupes' instead.`,
		Examples: []string{
			"dedupe list",
			"dedupe list --count 10",
			"dedupe list --min-size 1G",
			"dedupe list --min-size 500M",
		},
	},
	{
		Name:        "prune",
		Description: "Remove entries for files that no longer exist",
		Usage:       "prune",
		Help: `Remove database entries for files that no longer exist on disk.

This command helps keep the database in sync with the actual filesystem.`,
		Examples: []string{
			"dedupe prune",
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
			"dedupe organize --move /backup/dupes",
			"dedupe organize --run",
			"dedupe organize --strip-prefix /data",
		},
	},
	{
		Name:        "dedupe",
		Description: "Move duplicate files to a destination directory (deprecated, use 'files list-dupes --dest DIR' instead)",
		Usage:       "dedupe --dest DIR [--run] [--strip-prefix PREFIX] [--count N]",
		Help: `Move duplicate files to a destination directory.

Options:
  --dest DIR          Directory to move duplicates to (required)
  --run              Actually move files (default is dry-run)
  --strip-prefix PREFIX  Remove prefix from paths when moving
  --count N          Process only N duplicate groups (0 = unlimited)
  --ignore-dest      Ignore files already in destination (default: true)

By default, this runs in dry-run mode and only shows what would be done.

Note: This command is deprecated. Please use 'files list-dupes --dest DIR' instead.`,
		Examples: []string{
			"dedupe dedupe --dest /backup/dupes",
			"dedupe dedupe --dest /backup/dupes --run",
			"dedupe dedupe --dest /backup/dupes --strip-prefix /data",
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
			"dedupe listen",
		},
	},
	{
		Name:        "queue version",
		Description: "Publish a version update message to notify running instances",
		Usage:       "queue version [--version VERSION]",
		Help:        `Publish a version update message to notify running instances.`,
		Examples: []string{
			"dedupe queue version",
			"dedupe queue version --version 1.2.0",
		},
	},
	{
		Name:        "files",
		Description: "File-related commands (find, list-dupes, move-dupes)",
		Usage:       "files [find|list-dupes|move-dupes] [options]",
		Help: `File-related commands for finding and managing files.

Subcommands:
  find       - Find files for a specific host
  list-dupes - List duplicate files with optional deduplication
  move-dupes - Move duplicate files to a target directory

Options for list-dupes:
  --count N           Limit output to N duplicate groups (0 = unlimited)
  --min-size SIZE     Minimum file size to consider (e.g., "1M", "1.5G", "500K")
  --dest DIR          Directory to move duplicates to (if specified)
  --run               Actually move files (default is dry-run)
  --strip-prefix PREFIX  Remove prefix from paths when moving
  --ignore-dest       Ignore files already in destination (default: true)

Examples:
  dedupe files find
  dedupe files list-dupes --count 10
  dedupe files list-dupes --min-size 1G
  dedupe files list-dupes --dest /backup/dupes --run`,
		Examples: []string{
			"dedupe files find",
			"dedupe files list-dupes --count 10",
			"dedupe files list-dupes --min-size 1G",
			"dedupe files list-dupes --dest /backup/dupes --run",
		},
	},
	{
		Name:        "problematic",
		Description: "List files that timed out during hashing",
		Usage:       "problematic",
		Help: `List files that were marked as problematic due to timeout errors during hashing.

These files can be retried using the 'hash --retry-problematic' command.

The list shows the file ID, last attempt time, file size, and path.`,
		Examples: []string{
			"dedupe problematic",
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
