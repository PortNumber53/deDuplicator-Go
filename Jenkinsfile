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
    REMOTE_LOCK_DIR = "/var/lock/deduplicator"
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
            chmod +x scripts/deploy.sh
            scripts/deploy.sh
          '''
        }
      }
    }
  }
}

