pipeline {
  agent any
  options {
    timestamps()
  }

  environment {
    GO_VERSION = "1.24.0"
    HOSTS_AMD64 = "brain pinky"
    HOSTS_ARM64 = "rpi4"
    PRIMARY_DB_HOST = "brain"
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Setup Go') {
      steps {
        sh '''
          if ! command -v go >/dev/null 2>&1; then
            echo "Go is required on the Jenkins agent. Install Go ${GO_VERSION} or use an image with Go preinstalled."
            exit 1
          fi
          go version
        '''
      }
    }

    stage('Test') {
      steps {
        sh '''
          set -e
          GOCACHE=$(mktemp -d)
          export GOCACHE
          go test ./... -v
        '''
      }
    }

    stage('Build') {
      steps {
        sh '''
          mkdir -p dist
          CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/deduplicator-linux-amd64 .
          CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o dist/deduplicator-linux-arm64 .
        '''
      }
    }

    stage('Deploy') {
      environment {
        SSH_OPTS = "-o StrictHostKeyChecking=no"
      }
      steps {
        withCredentials([
          sshUserPrivateKey(credentialsId: 'Jenkins-private-key', keyFileVariable: 'SSH_KEY'),
          string(credentialsId: 'prod-database-url-godeduplicator', variable: 'DB_URL')
        ]) {
          sh '''
            set -e

            # Parse DB_URL into individual components for env/config
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
sudo install -m 755 /tmp/deduplicator /usr/local/bin/deduplicator
EOF
            }

            for h in ${HOSTS_AMD64}; do
              deploy_host "amd64" "$h" "dist/deduplicator-linux-amd64"
              if [ "$h" = "${PRIMARY_DB_HOST}" ]; then
                echo "Running migrations on ${h}"
                ssh -i "${SSH_KEY}" ${SSH_OPTS} "grimlock@${h}" <<EOF
export DB_HOST="${DB_HOST}"
export DB_PORT="${DB_PORT}"
export DB_USER="${DB_USER}"
export DB_PASSWORD="${DB_PASSWORD}"
export DB_NAME="${DB_NAME}"
deduplicator migrate up
EOF
              fi
            done

            for h in ${HOSTS_ARM64}; do
              deploy_host "arm64" "$h" "dist/deduplicator-linux-arm64"
            done
          '''
        }
      }
    }
  }
}

