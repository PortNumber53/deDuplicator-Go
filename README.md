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

- `createdb`: Initialize or recreate the database tables
  - Options:
    - `--force`: Force recreation of existing tables

- `update`: Process file information from standard input
  - Use this to add new files to the database for duplicate checking

- `hash`: Calculate and store file hashes
  - Options:
    - `--force`: Force rehash of all files
    - `--count <n>`: Process only n files (0 for all files)

- `list`: Display duplicate files found in the database
  - Options:
    - `--host <hostname>`: Check duplicates for a specific host
    - `--all-hosts`: Check duplicates across all hosts (default: current host only)

- `prune`: Remove entries for files that no longer exist on the filesystem
  - Options:
    - `--host <hostname>`: Prune files from a specific host
    - `--all-hosts`: Prune files across all hosts (default: current host only)

## Configuration

The following environment variables can be configured in your `.env` file:

```env
DB_HOST=localhost      # PostgreSQL host (default: localhost)
DB_PORT=5432          # PostgreSQL port (default: 5432)
DB_USER=postgres      # PostgreSQL user (default: postgres)
DB_NAME=deduplicator  # Database name (default: deduplicator)
DB_PASSWORD=          # PostgreSQL password (required)
```

## How It Works

The tool uses a PostgreSQL database to store file information and their hashes. It implements a locking mechanism to prevent concurrent modifications to the database during critical operations.

Typical workflow:
1. Run `createdb` to initialize the database
2. Use `update` to add files to the database
3. Run `hash` to calculate file hashes
4. Use `list` to find duplicates
5. Periodically use `prune` to clean up entries for deleted files

## Notes

- The tool uses file locking to prevent concurrent modifications
- Each command that modifies the database (`createdb`, `update`, `hash`, `prune`) acquires an exclusive lock
- The `.env` file is optional but recommended for database configuration

## Examples

Here's how to use each command:

### Initialize the Database
```bash
# Create database tables
deduplicator createdb

# Force recreate tables (warning: this will delete existing data)
deduplicator createdb --force
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
deduplicator hash

# Force rehash all files
deduplicator hash --force

# Hash only the first 100 unhashed files
deduplicator hash --count 100
```

### Find Duplicates
```bash
# List duplicate files on current host
deduplicator list

# List duplicate files on a specific host
deduplicator list --host server1

# List duplicate files across all hosts
deduplicator list --all-hosts
```

### Clean Up Database
```bash
# Remove entries for non-existent files on current host
deduplicator prune

# Remove entries for non-existent files on a specific host
deduplicator prune --host server1

# Remove entries for non-existent files across all hosts
deduplicator prune --all-hosts
```
