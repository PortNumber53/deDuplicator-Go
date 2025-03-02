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

- `prune`: Remove entries for files that no longer exist on the filesystem

- `organize`: Organize duplicate files by moving them
  - Options:
    - `--run`: Actually move files (default is dry-run)
    - `--move DIR`: Move duplicates to this directory
    - `--strip-prefix PREFIX`: Remove prefix from paths when moving

- `manage`: Manage backup hosts (add/edit/delete/list)
  - Subcommands:
    - `list`: List all registered hosts
    - `add`: Add a new host
    - `edit`: Edit an existing host
    - `delete`: Remove a host

- `files`: File-related commands for finding and managing files
  - Subcommands:
    - `find`: Find files for a specific host
    - `list-dupes`: List duplicate files and optionally move them to a destination directory
    - `move-dupes`: Move duplicate files to a target directory
    - `hash`: Calculate and update file hashes in the database

  - Options for `files list-dupes`:
    - `--count N`: Limit output to N duplicate groups (0 = unlimited)
    - `--min-size SIZE`: Minimum file size to consider (e.g., "1M", "1.5G", "500K")
    - `--dest DIR`: Directory to move duplicates to (if specified)
    - `--run`: Actually move files (default is dry-run)
    - `--strip-prefix PREFIX`: Remove prefix from paths when moving
    - `--ignore-dest`: Ignore files already in destination (default: true)

  - Options for `files hash`:
    - `--force`: Rehash files even if they already have a hash
    - `--renew`: Recalculate hashes older than 1 week
    - `--retry-problematic`: Retry files that previously timed out
    - `--count N`: Process only N files (0 = unlimited)

## Configuration

The following environment variables can be configured in your `.env` file:

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
```

## How It Works

The tool uses a PostgreSQL database to store file information and their hashes. It implements a locking mechanism to prevent concurrent modifications to the database during critical operations.

Typical workflow:
1. Run `migrate up` to initialize the database
2. Use `manage add` to add hosts to the database
3. Use `update` to add files to the database
4. Run `files hash` to calculate file hashes
5. Use `files list-dupes` to find duplicates
6. Optionally use `files list-dupes --dest DIR --run` to move duplicate files
7. Periodically use `prune` to clean up entries for deleted files

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
# List all hosts
deduplicator manage list

# Add a new host
deduplicator manage add myhost example.com 192.168.1.100 /data

# Edit an existing host
deduplicator manage edit myhost newhost.com 192.168.1.101 /backup

# Delete a host
deduplicator manage delete myhost
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
deduplicator prune
```

### Organize Files
```bash
# Show what would be organized (dry run)
deduplicator organize --move /backup/dupes

# Actually move files
deduplicator organize --move /backup/dupes --run

# Strip prefix from paths when moving
deduplicator organize --move /backup/dupes --strip-prefix /data --run
```
