#!/usr/bin/env bash
set -euo pipefail

: "${DB_URL:?DB_URL is required}"
: "${SSH_KEY:?SSH_KEY is required}"

SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=no}"
HOSTS_AMD64="${HOSTS_AMD64:-}"
HOSTS_ARM64="${HOSTS_ARM64:-}"
PRIMARY_DB_HOST="${PRIMARY_DB_HOST:-}"
REMOTE_LOCK_DIR="${REMOTE_LOCK_DIR:-/var/lock/deduplicator}"
BINARY_AMD64="${BINARY_AMD64:-dist/deduplicator-linux-amd64}"
BINARY_ARM64="${BINARY_ARM64:-dist/deduplicator-linux-arm64}"
LOCAL_MIGRATE_LOCK_DIR="${LOCAL_MIGRATE_LOCK_DIR:-/tmp/deduplicator-ci}"

# Parse DB_URL into component env vars for app and config
eval "$(python - <<'PY'
import os, urllib.parse
url = os.environ.get("DB_URL", "")
if not url:
    raise SystemExit("DB_URL is required")
p = urllib.parse.urlparse(url)
host = p.hostname or ""
port = p.port or 5432
user = p.username or ""
password = p.password or ""
dbname = (p.path or "").lstrip("/")
print(f"export DB_HOST='{host}' DB_PORT='{port}' DB_USER='{user}' DB_PASSWORD='{password}' DB_NAME='{dbname}'")
PY
)"

deploy_host() {
  arch="$1"; host="$2"; binary="$3"
  echo "Deploying to ${host} (arch=${arch})"

  scp -i "${SSH_KEY}" ${SSH_OPTS} "${binary}" "grimlock@${host}:/tmp/deduplicator"

  ssh -i "${SSH_KEY}" ${SSH_OPTS} "grimlock@${host}" <<EOF
set -e
sudo mkdir -p /etc/dedupe
sudo tee /etc/dedupe/config.ini >/dev/null <<CONFIG
[database]
url=${DB_URL}
host=${DB_HOST}
port=${DB_PORT}
user=${DB_USER}
password=${DB_PASSWORD}
name=${DB_NAME}
CONFIG
sudo mkdir -p "${REMOTE_LOCK_DIR}"
sudo chown grimlock:grimlock "${REMOTE_LOCK_DIR}"
sudo install -m 755 /tmp/deduplicator /usr/local/bin/deduplicator
EOF
}

# Run migrations once from Jenkins workspace (amd64 binary) before fan-out.
echo "Running migrations from Jenkins workspace"
DEDUPLICATOR_LOCK_DIR="${LOCAL_MIGRATE_LOCK_DIR}" \
DB_HOST="${DB_HOST}" \
DB_PORT="${DB_PORT}" \
DB_USER="${DB_USER}" \
DB_PASSWORD="${DB_PASSWORD}" \
DB_NAME="${DB_NAME}" \
"${BINARY_AMD64}" migrate up

# Deploy to AMD64 hosts
for h in ${HOSTS_AMD64}; do
  deploy_host "amd64" "$h" "${BINARY_AMD64}"
  if [[ -n "${PRIMARY_DB_HOST}" && "$h" == "${PRIMARY_DB_HOST}" ]]; then
    echo "Running migrations on ${h}"
    ssh -i "${SSH_KEY}" ${SSH_OPTS} "grimlock@${h}" <<EOF
export DB_HOST="${DB_HOST}"
export DB_PORT="${DB_PORT}"
export DB_USER="${DB_USER}"
export DB_PASSWORD="${DB_PASSWORD}"
export DB_NAME="${DB_NAME}"
export DEDUPLICATOR_LOCK_DIR="${REMOTE_LOCK_DIR}"
deduplicator migrate up
EOF
  fi
done

# Deploy to ARM64 hosts
for h in ${HOSTS_ARM64}; do
  deploy_host "arm64" "$h" "${BINARY_ARM64}"
done


