## Local Development

### Versioning

Every pushed/deployed change must increase `VERSION` in `main.go`.
Jenkins runs `scripts/check-version-bump.sh` and fails the build if the current
version is not greater than the previous build/commit version.

### Backend Hot Reload

Install Air once:

```bash
go install github.com/air-verse/air@latest
```

Run the backend with rebuild/restart on Go changes:

```bash
air
```

When developing from a Mac or another machine that is not registered as a
deduplicator host, point the server at one of the indexed hosts:

```bash
DEDUPLICATOR_SERVER_HOST=Brain air
```

If the host is registered but macOS reports it as a `.local` hostname, set the
local override in `~/.config/dedupe/config.ini`:

```ini
[default]
hostname=book16
```

The Air config builds to `tmp/deduplicator` and runs:

```bash
deduplicator server --addr 0.0.0.0:19111
```

Remote-host development mode is read-only for deletes. Search still works, but
filesystem deletion is only enabled when the served indexed host matches the
machine running the backend.

If the configured local hostname is not found in the `hosts` table, server mode
falls back to read-only search across all indexed hosts. Use
`DEDUPLICATOR_SERVER_HOST=Brain air` to narrow search to one indexed host.

### Frontend Dev Server

In another terminal:

```bash
npm --prefix web install
npm --prefix web run dev
```

Vite listens on `0.0.0.0:19110` and proxies `/api` to the Air-managed Go backend on `0.0.0.0:19111`.

### Hash query index migration

Migration `000007_add_hash_query_indexes` adds indexes for the case-insensitive
host/size lookup and the default unhashed-file ID traversal. Apply it during a
maintenance window: pause hashing, indexing, and dedupe writers, then run
`deduplicator migrate up` from the directory containing `migrations/`.
The transactional index builds block writes until they finish.

After migration, run `ANALYZE files;` in PostgreSQL, inspect the hash batch query
with `EXPLAIN (ANALYZE, BUFFERS)`, and resume workloads. The down migration drops
only the two new indexes. No file data or hash-selection rules change.

`files hash` database operations honor Ctrl+C and SIGTERM. There is no new query
time limit; file reads retain their existing inactivity timeout.

### Cancelling file commands

Ctrl+C or SIGTERM stops file commands without visiting the remaining candidates
or logging cancellation as thousands of per-file failures. Database operations,
hashing, and SSH/rsync transfers receive the command context; ordinary per-file
errors retain their existing handling. No new automatic database timeout applies.

Find, prune, and stdin ingestion roll back unfinished transactions on shutdown.
Earlier committed batches remain. Rerunning the command processes unfinished work.
Completed filesystem changes are retained; an interrupted copy or a completed
move/delete whose index update was cancelled may require reindexing or pruning.
Local filesystem system calls can finish before cancellation is observed.
