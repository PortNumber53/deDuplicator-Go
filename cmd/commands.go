package cmd

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
  <name>         - Unique identifier for the host
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
		Usage:       "hash [--force] [--count N]",
		Help: `Calculate and store file hashes for deduplication.

Options:
  --force        Rehash files even if they already have a hash
  --count N      Process only N files (0 = unlimited)

Files are hashed using SHA256 for reliable duplicate detection.`,
		Examples: []string{
			"dedupe hash",
			"dedupe hash --force",
			"dedupe hash --count 1000",
		},
	},
	{
		Name:        "list",
		Description: "List duplicate files",
		Usage:       "list [--host HOST] [--all-hosts] [--count N] [--min-size SIZE]",
		Help: `List duplicate files in the system.

Options:
  --host HOST    Only show duplicates for specific host
  --all-hosts    Show duplicates across all hosts
  --count N      Limit output to N duplicate groups (0 = unlimited)
  --min-size SIZE  Minimum file size to consider (e.g., "1M", "1.5G", "500K")

Files are considered duplicates if they have the same hash value.
Size units: B (bytes), K/KB, M/MB, G/GB, T/TB (1K = 1024 bytes)`,
		Examples: []string{
			"dedupe list",
			"dedupe list --host myserver",
			"dedupe list --all-hosts",
			"dedupe list --count 10",
			"dedupe list --min-size 1G",
			"dedupe list --min-size 500M",
		},
	},
	{
		Name:        "prune",
		Description: "Remove entries for files that no longer exist",
		Usage:       "prune [--host HOST] [--all-hosts]",
		Help: `Remove database entries for files that no longer exist on disk.

Options:
  --host HOST    Only prune files from specific host
  --all-hosts    Prune files across all hosts (requires --i-am-sure)

This command helps keep the database in sync with the actual filesystem.`,
		Examples: []string{
			"dedupe prune",
			"dedupe prune --host myserver",
			"dedupe prune --all-hosts --i-am-sure",
		},
	},
	{
		Name:        "organize",
		Description: "Organize duplicate files by moving them",
		Usage:       "organize [--host HOST] [--all-hosts] [--run] [--move DIR] [--strip-prefix PREFIX]",
		Help: `Organize duplicate files by moving them to a new location.

Options:
  --host HOST          Only organize files from specific host
  --all-hosts         Organize files across all hosts
  --run               Actually move files (default is dry-run)
  --move DIR          Move duplicates to this directory
  --strip-prefix PREFIX  Remove prefix from paths when moving

By default, this runs in dry-run mode and only shows what would be done.`,
		Examples: []string{
			"dedupe organize --move /backup/dupes",
			"dedupe organize --host myserver --run",
			"dedupe organize --all-hosts --strip-prefix /data",
		},
	},
	{
		Name:        "dedupe",
		Description: "Move duplicate files to a destination directory",
		Usage:       "dedupe --dest DIR [--run] [--strip-prefix PREFIX] [--count N]",
		Help: `Move duplicate files to a destination directory.

Options:
  --dest DIR          Directory to move duplicates to (required)
  --run              Actually move files (default is dry-run)
  --strip-prefix PREFIX  Remove prefix from paths when moving
  --count N          Process only N duplicate groups (0 = unlimited)
  --ignore-dest      Ignore files already in destination (default: true)

By default, this runs in dry-run mode and only shows what would be done.`,
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
		Help: `Publish a version update message to notify running instances.

Options:
  --version VERSION   Version number to publish (defaults to current version)

This command publishes a message to RabbitMQ that will notify all listening
instances to shut down gracefully.

Requires RabbitMQ environment variables to be set.`,
		Examples: []string{
			"dedupe queue version",
			"dedupe queue version --version 1.1.0",
		},
	},
	{
		Name:        "files",
		Description: "List and manage files",
		Usage:       "files [list|find|move-dupes] [options]",
		Help: `Manage and analyze files in the system.

Subcommands:
  list           - List duplicate files and potential space savings
  find           - Scan and index files from a host
  move-dupes     - Move duplicate files to a target directory

Options for find:
  --host         - Host to find files for (defaults to current host)

Options for list and move-dupes:
  --min-size     - Minimum file size to consider (default: 1MB)
  --host         - Filter duplicates by specific host
  --all-hosts    - Show duplicates across all hosts
  --count N      - Limit output to N duplicate groups

Additional options for move-dupes:
  --target       - Target directory to move duplicates to (required)
  --dry-run      - Show what would be moved without making changes

The list and move-dupes commands show duplicate files based on their content hash.
When moving files, the host's root path is stripped from the destination path.`,
		Examples: []string{
			"dedupe files list",
			"dedupe files list --min-size 10MB",
			"dedupe files list --host myserver",
			"dedupe files find",
			"dedupe files find --host myserver",
			"dedupe files move-dupes --target /backup/dupes",
			"dedupe files move-dupes --host myserver --target /backup/dupes",
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
