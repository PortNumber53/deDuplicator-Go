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

Path Group Subcommands:
  group-add <group name> [--min-copies N] [--max-copies N] [--description "..."] - Create a new path group
  group-list                                  - List all path groups
  group-show <group name>                     - Show detailed information about a path group
  group-delete <group name>                   - Delete a path group
  group-add-path <group name> <host name> <friendly path> [--priority N] - Add a path to a group
  group-remove-path <host name> <friendly path> - Remove a path from its group

Arguments:
  <server name>         - Friendly name for the server
  <hostname>            - DNS hostname or IP address
  <ip>                  - IP address (optional)
  <friendly path name>  - Friendly name for the path
  <absolute path>       - Absolute path on the server
  <group name>          - Name for the path group
  <priority>            - Priority for keeping files (lower = higher priority, default: 100)`,
		Examples: []string{
			"deduplicator manage server-list",
			"deduplicator manage server-add \"Backup1\" --hostname backup1.example.com --ip 192.168.1.10",
			"deduplicator manage server-edit \"Backup1\" --hostname backup1.local --ip 192.168.1.11",
			"deduplicator manage server-delete \"Backup1\"",
			"deduplicator manage path-list \"Backup1\"",
			"deduplicator manage path-add \"Backup1\" \"HomeDir\" \"/home/user\"",
			"deduplicator manage path-edit \"Backup1\" \"HomeDir\" \"/mnt/storage\"",
			"deduplicator manage path-delete \"Backup1\" \"HomeDir\"",
			"deduplicator manage group-add photos --min-copies 2 --max-copies 3 --description \"Family photos\"",
			"deduplicator manage group-list",
			"deduplicator manage group-show photos",
			"deduplicator manage group-add-path photos brain photos --priority 10",
			"deduplicator manage group-add-path photos pinky photos --priority 50",
			"deduplicator manage group-remove-path brain photos",
			"deduplicator manage group-delete photos",
		},
	},
	{
		Name:        "manage server-list",
		Description: "List all registered servers",
		Usage:       "manage server-list",
		Help: `List all servers registered in the database.`,
		Examples: []string{
			"deduplicator manage server-list",
		},
	},
	{
		Name:        "manage server-add",
		Description: "Add a new server",
		Usage:       "manage server-add \"Friendly server name\" --hostname <hostname> [--ip <ip>]",
		Help: `Add a new server/host to the database.

Options:
  --hostname <hostname>   DNS hostname or IP address (required)
  --ip <ip>               IP address (optional)`,
		Examples: []string{
			"deduplicator manage server-add \"Backup1\" --hostname backup1.example.com --ip 192.168.1.10",
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
		Name:        "manage server-delete",
		Description: "Remove a server",
		Usage:       "manage server-delete \"Friendly server name\"",
		Help:        `Delete a server/host from the database.`,
		Examples: []string{
			"deduplicator manage server-delete \"Backup1\"",
		},
	},
	{
		Name:        "manage path-list",
		Description: "List all paths for a server",
		Usage:       "manage path-list <server name>",
		Help:        `List all friendly path mappings for a given server.`,
		Examples: []string{
			"deduplicator manage path-list \"Backup1\"",
		},
	},
	{
		Name:        "manage path-add",
		Description: "Add a path mapping to a server",
		Usage:       "manage path-add <server name> <friendly path name> <absolute path>",
		Help:        `Add a friendly path name mapped to an absolute path on the specified server.`,
		Examples: []string{
			"deduplicator manage path-add \"Backup1\" \"Photos\" \"/mnt/photos\"",
		},
	},
	{
		Name:        "manage path-edit",
		Description: "Edit a path mapping for a server",
		Usage:       "manage path-edit <server name> <friendly path name> <new absolute path>",
		Help:        `Update the absolute path associated with a friendly path name on the specified server.`,
		Examples: []string{
			"deduplicator manage path-edit \"Backup1\" \"Photos\" \"/mnt/storage/photos\"",
		},
	},
	{
		Name:        "manage path-delete",
		Description: "Remove a path mapping from a server",
		Usage:       "manage path-delete <server name> <friendly path name>",
		Help:        `Remove a friendly path mapping from the specified server.`,
		Examples: []string{
			"deduplicator manage path-delete \"Backup1\" \"Photos\"",
		},
	},
	{
		Name:        "manage group-add",
		Description: "Create a new path group",
		Usage:       "manage group-add <group name> [--min-copies N] [--max-copies N] [--description \"...\"]",
		Help: `Create a path group to deduplicate/balance duplicates across multiple hosts.

Options:
  --min-copies N         Minimum copies to maintain (default: 2)
  --max-copies N         Maximum copies to keep (default: unlimited)
  --description \"text\"  Description of the group`,
		Examples: []string{
			"deduplicator manage group-add photos --min-copies 2 --max-copies 3 --description \"Family photos\"",
		},
	},
	{
		Name:        "manage group-list",
		Description: "List all path groups",
		Usage:       "manage group-list",
		Help:        `List all configured path groups.`,
		Examples: []string{
			"deduplicator manage group-list",
		},
	},
	{
		Name:        "manage group-show",
		Description: "Show details of a path group",
		Usage:       "manage group-show <group name>",
		Help:        `Show group settings and its member paths.`,
		Examples: []string{
			"deduplicator manage group-show photos",
		},
	},
	{
		Name:        "manage group-delete",
		Description: "Delete a path group",
		Usage:       "manage group-delete <group name>",
		Help:        `Delete a path group and its membership.`,
		Examples: []string{
			"deduplicator manage group-delete photos",
		},
	},
	{
		Name:        "manage group-add-path",
		Description: "Add a path to a group (with optional priority)",
		Usage:       "manage group-add-path <group name> <host name> <friendly path> [--priority N]",
		Help: `Add a host's friendly path to a group.

Priority:
  - Lower numbers = higher priority to keep files (default: 100)`,
		Examples: []string{
			"deduplicator manage group-add-path photos brain photos --priority 10",
			"deduplicator manage group-add-path photos pinky photos --priority 50",
		},
	},
	{
		Name:        "manage group-remove-path",
		Description: "Remove a path from its group",
		Usage:       "manage group-remove-path <host name> <friendly path>",
		Help:        `Remove a host path from whatever group it belongs to.`,
		Examples: []string{
			"deduplicator manage group-remove-path brain photos",
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
		Usage:       "files [find|list-dupes|move-dupes|hash|prune|import|mirror|dedupe-group] [options]",
		Help: `Manage file operations including finding, hashing, and duplicate detection.

Subcommands:
  find        - Search for files based on criteria
  list-dupes  - List duplicate files
  move-dupes  - Move duplicate files to a destination
  hash        - Calculate and store file hashes
  prune       - Remove entries for files that no longer exist
  import      - Import files from another location
  mirror      - Mirror a friendly path (implementation-specific)
  dedupe-group - Balance/limit duplicates across a path group

Use 'files <subcommand> --help' for more information on a specific subcommand.`,
		Examples: []string{
			"deduplicator files find",
			"deduplicator files list-dupes --count 10",
			"deduplicator files list-dupes --min-size 1G",
			"deduplicator files move-dupes --target /backup/dupes",
			"deduplicator files move-dupes --target /backup/dupes --dry-run",
			"deduplicator files hash --force",
			"deduplicator files prune",
			"deduplicator files import --source /path/to/files --server myhost --path Photos",
			"deduplicator files mirror Photos",
			"deduplicator files dedupe-group photos --dry-run",
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
		Name:        "files hash",
		Description: "Calculate and store file hashes for the current host",
		Usage:       "files hash [--force] [--renew] [--retry-problematic]",
		Help: `Calculate and store file hashes for deduplication (host is inferred from OS hostname).

Options:
  --force              Rehash files even if they already have a hash
  --renew              Recalculate hashes older than 1 week
  --retry-problematic  Retry files that previously timed out`,
		Examples: []string{
			"deduplicator files hash",
			"deduplicator files hash --force",
			"deduplicator files hash --retry-problematic",
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
		Usage:       "files import --source DIR --server NAME --path PATH [options]",
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
  --count N          Limit the number of files to process (0 = no limit, default: 0)
  --age MINUTES      Only import files older than this many minutes`,
		Examples: []string{
			"deduplicator files import --source /path/to/files --server myhost --path Photos",
			"deduplicator files import --source /path/to/files --server myhost --path Photos --remove-source",
			"deduplicator files import --source /path/to/files --server myhost --path Photos --dry-run",
		},
	},
	{
		Name:        "files list-dupes",
		Description: "List duplicates (or move them if --dest is provided)",
		Usage:       "files list-dupes [--count N] [--min-size SIZE] [--dest DIR] [--run] [--strip-prefix PREFIX] [--ignore-dest=true|false]",
		Help: `List duplicates for the current host (host inferred from OS hostname).

If --dest is provided, duplicates are moved (dry-run by default; use --run to actually move).

Options:
  --count N             Limit number of duplicate groups shown (0 = unlimited)
  --min-size SIZE       Minimum file size (e.g. 1M, 1.5G, 500K)
  --dest DIR            Directory to move duplicates to (optional)
  --run                 Actually move files (default is dry-run)
  --strip-prefix PREFIX Remove this prefix from paths when moving
  --ignore-dest         Ignore files already in destination dir (default: true)`,
		Examples: []string{
			"deduplicator files list-dupes --count 10",
			"deduplicator files list-dupes --min-size 1G",
			"deduplicator files list-dupes --dest /backup/dupes",
			"deduplicator files list-dupes --dest /backup/dupes --run",
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
	{
		Name:        "files mirror",
		Description: "Mirror a friendly path (implementation-specific)",
		Usage:       "files mirror <friendly path>",
		Help:        `Mirror a friendly path. This is an advanced command and may depend on your deployment setup.`,
		Examples: []string{
			"deduplicator files mirror Photos",
		},
	},
	{
		Name:        "files dedupe-group",
		Description: "Balance/limit duplicates across a path group",
		Usage:       "files dedupe-group <group name> [--balance-mode MODE] [--respect-limits] [--dry-run|--run] [--min-size BYTES] [--count N]",
		Help: `Deduplicate files across all hosts/paths in a path group.

Options:
  --balance-mode <mode>  Balance mode: priority (default), equal, capacity
  --respect-limits       Honor min/max copy limits from group settings
  --dry-run              Show what would be done without making changes (default)
  --run                  Actually perform the deduplication
  --min-size <bytes>     Only process files larger than this size
  --count <n>            Limit the number of duplicate groups to process`,
		Examples: []string{
			"deduplicator files dedupe-group photos --dry-run",
			"deduplicator files dedupe-group photos --respect-limits --run",
		},
	},
	{
		Name:        "problematic",
		Description: "List problematic files for the current host",
		Usage:       "problematic",
		Help:        `List files that are marked problematic (e.g., timeouts) for the current host.`,
		Examples: []string{
			"deduplicator problematic",
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
