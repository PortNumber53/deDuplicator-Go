pipeline {
  agent any
  options {
    timestamps()
  }

  environment {
    GO_VERSION = "1.24.0"
    HOSTS_AMD64 = "brain pinky"
    HOSTS_ARM64 = "rpi4"
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
          sshUserPrivateKey(credentialsId: 'deduplicator-ssh', keyFileVariable: 'SSH_KEY'),
          string(credentialsId: 'prod-database-url-godeduplicator', variable: 'DB_URL')
        ]) {
          sh '''
            set -e
            deploy_host() {
              arch="$1"; host="$2"; binary="$3"
              echo "Deploying to ${host} (arch=${arch})"
              scp -i "${SSH_KEY}" ${SSH_OPTS} "${binary}" "grimlock@${host}:/tmp/deduplicator"
              ssh -i "${SSH_KEY}" ${SSH_OPTS} "grimlock@${host}" <<'EOF'
sudo mkdir -p /etc/dedupe
sudo tee /etc/dedupe/config.ini >/dev/null <<CONFIG
[database]
url=${DB_URL}
CONFIG
sudo install -m 755 /tmp/deduplicator /usr/local/bin/deduplicator
EOF
            }

            for h in ${HOSTS_AMD64}; do
              deploy_host "amd64" "$h" "dist/deduplicator-linux-amd64"
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

