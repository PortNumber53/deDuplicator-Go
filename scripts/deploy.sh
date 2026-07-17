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
CONFIG_SAMPLE="${CONFIG_SAMPLE:-config.ini.sample}"
FRONTEND_ARCHIVE="${FRONTEND_ARCHIVE:-dist/deduplicator-web.tar.gz}"
FRONTEND_REMOTE_DIR="${FRONTEND_REMOTE_DIR:-/usr/local/share/deduplicator/web}"
SKIP_TESTS="${SKIP_TESTS:-0}"
DEPLOY_TEST_CACHE_DIR=""
DEPLOY_CONFIG_FILE=""

cleanup_deploy() {
  if [[ -n "${DEPLOY_TEST_CACHE_DIR:-}" ]]; then
    rm -rf "${DEPLOY_TEST_CACHE_DIR}"
  fi
  if [[ -n "${DEPLOY_CONFIG_FILE:-}" ]]; then
    rm -f "${DEPLOY_CONFIG_FILE}"
  fi
}

run_tests() {
  if [[ "${SKIP_TESTS}" == "1" ]]; then
    echo "Skipping pre-deploy tests because SKIP_TESTS=1"
    return
  fi

  if ! command -v go >/dev/null 2>&1; then
    echo "Go is required to run pre-deploy tests" >&2
    exit 1
  fi

  echo "Running pre-deploy tests"
  local test_cache
  if [[ -n "${GOCACHE:-}" ]]; then
    test_cache="${GOCACHE}"
  else
    DEPLOY_TEST_CACHE_DIR="$(mktemp -d)"
    test_cache="${DEPLOY_TEST_CACHE_DIR}"
    trap cleanup_deploy EXIT
  fi
  GOCACHE="${test_cache}" go test ./...
}

run_tests

if [[ ! -f "${FRONTEND_ARCHIVE}" ]]; then
  echo "Frontend archive is required but was not found: ${FRONTEND_ARCHIVE}" >&2
  exit 1
fi
if [[ -z "${FRONTEND_REMOTE_DIR}" || "${FRONTEND_REMOTE_DIR}" == "/" ]]; then
  echo "Refusing unsafe FRONTEND_REMOTE_DIR: ${FRONTEND_REMOTE_DIR}" >&2
  exit 1
fi

# Parse DB_URL into component env vars for app and config
eval "$(python - <<'PY'
import os, shlex, urllib.parse
url = os.environ.get("DB_URL", "")
if not url:
    raise SystemExit("DB_URL is required")
p = urllib.parse.urlparse(url)
host = p.hostname or ""
port = p.port or 5432
user = p.username or ""
password = p.password or ""
dbname = (p.path or "").lstrip("/")
for key, value in {
    "DB_HOST": host,
    "DB_PORT": str(port),
    "DB_USER": user,
    "DB_PASSWORD": password,
    "DB_NAME": dbname,
}.items():
    print(f"export {key}={shlex.quote(value)}")
PY
)"

write_deploy_config() {
  DEPLOY_CONFIG_FILE="$(mktemp)"
  trap cleanup_deploy EXIT
  {
    printf '[database]\n'
    printf 'url=%s\n' "${DB_URL}"
    printf 'host=%s\n' "${DB_HOST}"
    printf 'port=%s\n' "${DB_PORT}"
    printf 'user=%s\n' "${DB_USER}"
    printf 'password=%s\n' "${DB_PASSWORD}"
    printf 'name=%s\n' "${DB_NAME}"

    if [[ -n "${RABBITMQ_HOST:-}" ]]; then
      printf '\n[rabbitmq]\n'
      printf 'host=%s\n' "${RABBITMQ_HOST}"
      printf 'port=%s\n' "${RABBITMQ_PORT:-5672}"
      printf 'user=%s\n' "${RABBITMQ_USER:-guest}"
      if [[ -n "${RABBITMQ_PASSWORD:-}" ]]; then
        printf 'password=%s\n' "${RABBITMQ_PASSWORD}"
      fi
      printf 'vhost=%s\n' "${RABBITMQ_VHOST:-/}"
    fi
  } > "${DEPLOY_CONFIG_FILE}"
}

write_deploy_config

deploy_host() {
  arch="$1"; host="$2"; binary="$3"
  echo "Deploying to ${host} (arch=${arch})"

  scp -i "${SSH_KEY}" ${SSH_OPTS} "${binary}" "grimlock@${host}:/tmp/deduplicator-binary"
  scp -i "${SSH_KEY}" ${SSH_OPTS} "${CONFIG_SAMPLE}" "grimlock@${host}:/tmp/dedupe-config.ini.sample"
  scp -i "${SSH_KEY}" ${SSH_OPTS} "${DEPLOY_CONFIG_FILE}" "grimlock@${host}:/tmp/dedupe-config.ini"
  scp -i "${SSH_KEY}" ${SSH_OPTS} "${FRONTEND_ARCHIVE}" "grimlock@${host}:/tmp/deduplicator-web.tar.gz"

  ssh -i "${SSH_KEY}" ${SSH_OPTS} "grimlock@${host}" <<EOF
set -e
sudo mkdir -p /etc/dedupe
sudo install -m 644 /tmp/dedupe-config.ini.sample /etc/dedupe/config.ini.sample
sudo install -m 640 -o root -g grimlock /tmp/dedupe-config.ini /etc/dedupe/config.ini
sudo mkdir -p "${REMOTE_LOCK_DIR}"
sudo chown grimlock:grimlock "${REMOTE_LOCK_DIR}"
sudo install -m 755 /tmp/deduplicator-binary /usr/local/bin/deduplicator
sudo rm -rf "${FRONTEND_REMOTE_DIR}"
sudo mkdir -p "${FRONTEND_REMOTE_DIR}"
sudo tar -xzf /tmp/deduplicator-web.tar.gz -C "${FRONTEND_REMOTE_DIR}"
sudo chmod -R a+rX "${FRONTEND_REMOTE_DIR}"
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
export DEDUPLICATOR_LOCK_DIR="${REMOTE_LOCK_DIR}"
deduplicator migrate up
EOF
  fi
done

# Deploy to ARM64 hosts
for h in ${HOSTS_ARM64}; do
  deploy_host "arm64" "$h" "${BINARY_ARM64}"
done
