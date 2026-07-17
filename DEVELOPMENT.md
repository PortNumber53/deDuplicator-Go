## Local Development

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
