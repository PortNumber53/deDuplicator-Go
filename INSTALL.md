# Installation Instructions

## Prerequisites

- Go 1.21 or later
- PostgreSQL
- Systemd (for automated file processing)

## Database Setup

1. Create a PostgreSQL database:
```bash
createdb deduplicator
```

2. Configure database connection in `.env`:
```bash
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_NAME=deduplicator
# DB_PASSWORD=your_password  # Uncomment and set if needed
```

3. Initialize the database:
```bash
go run main.go createdb
```

## Systemd Setup (Optional)

The deduplicator includes a systemd service that automatically processes files in batches. To set it up:

1. Install the systemd files:
```bash
./scripts/install-systemd.sh
```

2. Enable and start the timer:
```bash
systemctl --user enable deduplicator-hash.timer
systemctl --user start deduplicator-hash.timer
```

The service will:
- Process up to 100 files per run
- Run every minute
- Start 5 minutes after system boot
- Run as your user account

### Checking Service Status

View timer status:
```bash
systemctl --user status deduplicator-hash.timer
```

View service logs:
```bash
journalctl --user -u deduplicator-hash.service
```

### Modifying Service Settings

The systemd files are located in the `systemd/` directory:
- `deduplicator-hash.service`: Service configuration
- `deduplicator-hash.timer`: Timer configuration

After modifying these files, reinstall them:
```bash
./scripts/install-systemd.sh
```

And restart the timer:
```bash
systemctl --user restart deduplicator-hash.timer
```

## Manual Usage

Without systemd, you can:

1. Add files to the database:
```bash
find /path/to/files -type f | go run main.go update
```

2. Calculate file hashes:
```bash
go run main.go hash --count 100
```

3. List duplicate files:
```bash
go run main.go list
```
