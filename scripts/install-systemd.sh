#!/bin/bash
set -e

# Get the absolute path to the project directory
PROJECT_DIR="$(dirname "$(dirname "$(readlink -f "$0")")")"

# Create systemd user directory if it doesn't exist
mkdir -p ~/.config/systemd/user/

# Create symlinks for systemd files
ln -sf "$PROJECT_DIR/systemd/deduplicator-hash.service" ~/.config/systemd/user/
ln -sf "$PROJECT_DIR/systemd/deduplicator-hash.timer" ~/.config/systemd/user/

# Reload systemd user daemon
systemctl --user daemon-reload

echo "Systemd files installed successfully!"
echo "To enable and start the timer, run:"
echo "systemctl --user enable deduplicator-hash.timer"
echo "systemctl --user start deduplicator-hash.timer"
