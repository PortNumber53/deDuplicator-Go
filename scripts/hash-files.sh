#!/bin/bash
set -e

# Change to the deduplicator directory
cd "$(dirname "$(dirname "$(readlink -f "$0")")")"

# Source the .env file if it exists
if [ -f .env ]; then
    set -a  # automatically export all variables
    source .env
    set +a
fi

# Run the hash command
go run main.go hash --count 100
