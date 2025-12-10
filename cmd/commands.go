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
		Description: "Manage backup servers and their paths",
		Usage:       "manage <subcommand> [options]",
		Help: `Manage backup servers and their associated paths.

Server Subcommands:
  server-list                                 - List all registered servers
  server-add "Friendly server name" --hostname <hostname> [--ip <ip>]   - Add a new server
  server-edit "Current friendly name" [--new-friendly-name <new name>] [--hostname <hostname>] [--ip <ip>] - Edit an existing server
  server-delete "Friendly server name"         - Remove a server

Path Subcommands:
  path-list <server name>                     - List all paths for a server
  path-add <server name> <friendly path name> <absolute path>   - Add a path to a server
  path-edit <server name> <friendly path name> <new absolute path> - Edit a path for a server
  path-delete <server name> <friendly path name>                - Remove a path from a server

Arguments:
  <server name>         - Friendly name for the server
  <hostname>            - DNS hostname or IP address
  <ip>                  - IP address (optional)
  <friendly path name>  - Friendly name for the path
  <absolute path>       - Absolute path on the server`,
		Examples: []string{
			"deduplicator manage server-list",
			"deduplicator manage server-add \"Backup1\" --hostname backup1.example.com --ip 192.168.1.10",
			"deduplicator manage server-edit \"Backup1\" --hostname backup1.local --ip 192.168.1.11",
			"deduplicator manage server-delete \"Backup1\"",
			"deduplicator manage path-list \"Backup1\"",
			"deduplicator manage path-add \"Backup1\" \"HomeDir\" \"/home/user\"",
			"deduplicator manage path-edit \"Backup1\" \"HomeDir\" \"/mnt/storage\"",
			"deduplicator manage path-delete \"Backup1\" \"HomeDir\"",
		},
	},
	{
		Name:        "manage server-edit",
		Description: "Edit an existing server's details (friendly name, hostname, IP).",
		Usage:       "manage server-edit \"Current friendly name\" [--new-friendly-name <new name>] [--hostname <hostname>] [--ip <ip>]",
		Help: "Edit the details of an existing server registered in the database.\n\n" +
			"You must specify the server's current friendly name to identify it.\n\n" +
			"Options:\n" +
			"  --new-friendly-name <new name>  Set a new friendly name for the server.\n" +
			"  --hostname <hostname>           Set a new hostname for the server.\n" +
			"  --ip <ip>                       Set a new IP address for the server.\n\n" +
			"If an option is not provided, the corresponding value for the server will remain unchanged.",
		Examples: []string{
			"deduplicator manage server-edit \"Old Server Name\" --new-friendly-name \"New Server Name\"",
			"deduplicator manage server-edit \"My Server\" --hostname \"new.server.hostname.com\"",
			"deduplicator manage server-edit \"My Server\" --ip \"192.168.1.100\"",
			"deduplicator manage server-edit \"Server Alpha\" --new-friendly-name \"Server Beta\" --hostname \"beta.local\" --ip \"10.0.0.5\"",
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
		Description: "Calculate and update file hashes in the database (deprecated, use 'files hash' instead)",
		Usage:       "hash [--force] [--renew] [--retry-problematic] [--count N]",
		Help: `Calculate and store file hashes for deduplication.

Options:
  --force              Rehash files even if they already have a hash
  --renew              Recalculate hashes older than 1 week
  --retry-problematic  Retry files that previously timed out
  --count N            Process only N files (0 = unlimited)

Note: This command is deprecated. Please use 'files hash' instead.

Files are hashed using SHA256 for reliable duplicate detection.`,
		Examples: []string{
			"deduplicator hash",
			"deduplicator hash --force",
			"deduplicator hash --retry-problematic",
			"deduplicator hash --count 1000",
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
		Description: "Manage file operations (find, hashing, duplicate detection, pruning)",
		Usage:       "files [find|list-dupes|move-dupes|hash|prune|import] [options]",
		Help: `Manage file operations including finding, hashing, and duplicate detection.

Subcommands:
  find        - Search for files based on criteria
  list-dupes  - List duplicate files
  move-dupes  - Move duplicate files to a destination
  hash        - Calculate and store file hashes
  prune       - Remove entries for files that no longer exist
  import      - Import files from another location

Use 'files <subcommand> --help' for more information on a specific subcommand.`,
		Examples: []string{
			"deduplicator files find",
			"deduplicator files list-dupes --count 10",
			"deduplicator files list-dupes --min-size 1G",
			"deduplicator files move-dupes --target /backup/dupes",
			"deduplicator files move-dupes --target /backup/dupes --dry-run",
			"deduplicator files hash --force",
			"deduplicator files prune",
			"deduplicator files import --source /path/to/files --server myhost",
		},
	},
	{
		Name:        "files find",
		Description: "Search for files based on criteria",
		Usage:       "files find [--server HOSTNAME] [--path PATH_NAME]",
		Help: `Search for files in the database based on specified criteria.

Options:
  --server HOSTNAME  Host to find files for (defaults to current host)
  --path PATH_NAME   Friendly path name to search within (optional)`,
		Examples: []string{
			"deduplicator files find",
			"deduplicator files find --server myhost",
			"deduplicator files find --server myhost --path 'My Documents'",
		},
	},
	{
		Name:        "files prune",
		Description: "Remove entries for files that no longer exist",
		Usage:       "files prune",
		Help: `Remove database entries for files that no longer exist on disk.

This command helps keep the database in sync with the actual filesystem.`,
		Examples: []string{
			"deduplicator files prune",
		},
	},
	{
		Name:        "files import",
		Description: "Import files from a source directory to a target host",
		Usage:       "files import --source DIR --server NAME [options]",
		Help: `Import files from a source directory to a target host.

The command transfers files using rsync and adds them to the database.
Files that already exist on the target host (based on hash) will be skipped.

Options:
  --source DIR        Source directory to import files from (required)
  --server NAME      Target server to import files to (required)
  --path PATH        Friendly path on the target server (required)
  --duplicate DIR    Move duplicate files to this directory instead of skipping
  --remove-source     Remove source files after successful import
  --dry-run          Show what would be imported without making changes
  --count N          Limit the number of files to process (0 = no limit, default: 0)`,
		Examples: []string{
			"deduplicator files import --source /path/to/files --server myhost",
			"deduplicator files import --source /path/to/files --server myhost --remove-source",
			"deduplicator files import --source /path/to/files --server myhost --dry-run",
		},
	},
	{
		Name:        "files move-dupes",
		Description: "Move duplicate files to a specified target directory",
		Usage:       "files move-dupes --target TARGET_DIR [--dry-run]",
		Help: `Move duplicate files to a specified target directory.

This command identifies duplicate files in the database and moves all but one copy
to the specified target directory. By default, it runs in dry-run mode to show
what would be moved without making any changes.

Options:
  --target string   Target directory where duplicate files will be moved (required)
  --dry-run         Show what would be moved without making any changes (default: false)
  --help            Show help for move-dupes command

Note: The original directory structure will be preserved under the target directory.`,
		Examples: []string{
			"# Show what would be moved (dry run)",
			"deduplicator files move-dupes --target /backup/dupes --dry-run",
			"",
			"# Actually move duplicate files",
			"deduplicator files move-dupes --target /backup/dupes",
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
