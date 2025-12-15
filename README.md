# Deduplicator

A command-line tool for finding and managing duplicate files in your filesystem.

## Installation

1. Ensure you have PostgreSQL installed and running
2. Set up your environment variables (see [Configuration](#configuration))
3. Build and install the tool:

```bash
# Clone the repository
git clone https://github.com/yourusername/deduplicator.git
cd deduplicator

# Build the binary
go build -o deduplicator

# Optional: Install system-wide (requires appropriate permissions)
sudo mv deduplicator /usr/local/bin/

# Or add to your local bin directory
mkdir -p ~/bin
mv deduplicator ~/bin/
export PATH="$HOME/bin:$PATH"  # Add this to your ~/.bashrc or ~/.zshrc
```

## Usage

The deduplicator tool provides several commands to help you manage duplicate files:

### Commands

- `migrate`: Run database migrations
  - Subcommands:
    - `up`: Apply all pending migrations
    - `down`: Roll back the last applied migration
    - `reset`: Drop all tables and reapply migrations
    - `status`: Show current migration status

- `createdb`: Initialize or recreate the database schema (deprecated, use `migrate up` instead)
  - Options:
    - `--force`: Force recreation of existing tables

- `update`: Process file paths from stdin and update the database
  - Use this to add new files to the database for duplicate checking

- `files`: File-related commands
  - Subcommands:
    - `find`: Find files for a specific host
    - `list-dupes`: List duplicate files
    - `move-dupes`: Move duplicate files to a target directory
      - Options:
        - `--target DIR`: Target directory to move duplicates to (required)
        - `--dry-run`: Show what would be moved without making changes (default)
        - `--min-size SIZE`: Minimum file size to consider (e.g., "1M", "1.5G", "500K")
    - `hash`: Calculate and update file hashes in the database
      - Options:
        - `--force`: Rehash files even if they already have a hash
        - `--renew`: Recalculate hashes older than 1 week
        - `--retry-problematic`: Retry files that previously timed out
        - `--count N`: Process only N files (0 = unlimited)
    - `import`: Import files from a source directory to a target host
      - Options:
        - `--source DIR`: Source directory to import files from (required)
        - `--server NAME`: Target server to import files to (required)
        - `--path FRIENDLY`: Friendly path on the target server (required)
        - `--remove-source`: Remove source files after successful import
        - `--dry-run`: Show what would be imported without making changes
        - `--count N`: Limit the number of files to process (0 = no limit)
        - `--duplicate DIR`: Move duplicates into this directory instead of skipping them
        - `--age MINUTES`: Only import files older than this many minutes

- `manage`: Manage servers and their configured paths
  - Subcommands:
    - `server-list`: List all registered servers
    - `server-add`: Add a new server
    - `server-edit`: Edit an existing server
    - `server-delete`: Remove a server
    - `path-list`: List paths for a server
    - `path-add`: Add a path to a server
    - `path-edit`: Edit a path on a server
    - `path-delete`: Remove a path from a server

- `problematic`: List problematic files for the current host (timeouts/errors during hashing)

- `listen` / `queue version`: Optional RabbitMQ commands for version update notifications

## Configuration

Deduplicator reads DB configuration from (highest priority first):

1. Existing environment variables (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`)
2. A local `.env` file in the current working directory (optional)
3. `/etc/dedupe/config.ini` (optional; created by the Jenkins deploy)

### `/etc/dedupe/config.ini`

Example:

```ini
[database]
# Either provide a URL:
url=postgres://user:pass@dbhost:5432/dbname?sslmode=disable

# Or provide individual fields:
host=dbhost
port=5432
user=user
password=pass
name=dbname
```

### Environment variables

The following environment variables can be configured in your `.env` file (or exported in your shell):

```env
DB_HOST=localhost      # PostgreSQL host (default: localhost)
DB_PORT=5432          # PostgreSQL port (default: 5432)
DB_USER=postgres      # PostgreSQL user (default: postgres)
DB_NAME=deduplicator  # Database name (default: deduplicator)
DB_PASSWORD=          # PostgreSQL password (required)
RABBITMQ_HOST=        # RabbitMQ host (optional)
RABBITMQ_PORT=5672    # RabbitMQ port (default: 5672)
RABBITMQ_VHOST=       # RabbitMQ vhost
RABBITMQ_USER=        # RabbitMQ username
RABBITMQ_PASSWORD=    # RabbitMQ password
RABBITMQ_QUEUE=dedup_backup  # RabbitMQ queue name (default: dedup_backup)
DEDUPLICATOR_LOCK_DIR=/var/lock/deduplicator  # Override lock directory (default: /tmp/deduplicator)
LOCAL_MIGRATE_LOCK_DIR=/var/lock/deduplicator # Back-compat lock dir override for migrations
```

## How It Works

The tool uses a PostgreSQL database to store file information and their hashes. It implements a locking mechanism to prevent concurrent modifications to the database during critical operations.

Typical workflow:
1. Run `migrate up` to initialize the database
2. Use `manage server-add` and `manage path-add` to register servers and paths
3. Use `update` to add files to the database
4. Run `files hash` to calculate file hashes
5. Use `files list-dupes` to find duplicates
6. Optionally use `files list-dupes --dest DIR --run` to move duplicate files
7. Periodically use `files prune` to clean up entries for deleted files

## Notes

- The tool uses file locking to prevent concurrent modifications
- Each command that modifies the database acquires an exclusive lock
- The `.env` file is optional but recommended for database configuration
- When moving duplicate files, the tool keeps the file in the directory with the most unique files

## Examples

Here's how to use each command:

### Initialize the Database
```bash
# Create database tables
deduplicator migrate up

# Roll back the last migration
deduplicator migrate down

# Show migration status
deduplicator migrate status
```

### Manage Hosts
```bash
# List all servers
deduplicator manage server-list

# Add a new server
deduplicator manage server-add "My Server" --hostname myhost.example.com --ip 192.168.1.100

# Add a friendly path to a server
deduplicator manage path-add "My Server" "Data" "/data"

# Edit an existing server
deduplicator manage server-edit "My Server" --hostname newhost.example.com --ip 192.168.1.101

# Delete a server
deduplicator manage server-delete "My Server"
```

### Add Files to Database
```bash
# Add all files from current directory recursively
find . -type f | deduplicator update

# Add specific directory
find /path/to/directory -type f | deduplicator update

# Add files with specific extensions
find . -type f -name "*.jpg" -o -name "*.png" | deduplicator update
```

### Calculate File Hashes
```bash
# Hash all files in database
deduplicator files hash

# Force rehash all files
deduplicator files hash --force

# Recalculate hashes older than 1 week
deduplicator files hash --renew

# Retry files that previously timed out
deduplicator files hash --retry-problematic
```

### Find Duplicates
```bash
# List duplicate files
deduplicator files list-dupes

# List top 10 duplicate groups by size
deduplicator files list-dupes --count 10

# List duplicates larger than 1GB
deduplicator files list-dupes --min-size 1G

# Move duplicate files to a destination directory
deduplicator files list-dupes --dest /backup/dupes --run

# Move duplicates with path prefix stripping
deduplicator files list-dupes --dest /backup/dupes --strip-prefix /data --run
```

### Clean Up Database
```bash
# Remove entries for non-existent files
deduplicator files prune
```

### Move Duplicate Files
```bash
# Show what would be moved (dry run)
deduplicator files move-dupes --target /backup/dupes --dry-run

# Actually move files
deduplicator files move-dupes --target /backup/dupes

# Only consider files larger than 1MB
deduplicator files move-dupes --target /backup/dupes --min-size 1M
```

### Import Files to a Remote Host
```bash
# Show what would be imported (dry run)
deduplicator files import --source /path/to/files --server "My Server" --path "Data" --dry-run

# Actually import files
deduplicator files import --source /path/to/files --server "My Server" --path "Data"

# Import files and remove source files after successful import
deduplicator files import --source /path/to/files --server "My Server" --path "Data" --remove-source

# Import only the first 10 files
deduplicator files import --source /path/to/files --server "My Server" --path "Data" --count 10

# Only import files older than 60 minutes (useful for “files still being written” avoidance)
deduplicator files import --source /path/to/files --server "My Server" --path "Data" --age 60
```
