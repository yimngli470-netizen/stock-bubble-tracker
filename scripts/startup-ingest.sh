#!/bin/bash
# Starts Docker Compose services for stock-bubble-tracker on login.
# The worker container handles backfill + daily collection automatically on startup.

set -euo pipefail

PROJECT_DIR="/Users/yiming/Desktop/stock-bubble-tracker"
LOG_FILE="$PROJECT_DIR/scripts/startup-ingest.log"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOG_FILE"
}

log "=== Startup triggered ==="

# Ensure Docker is running
DOCKER_CMD=""
for cmd in /usr/local/bin/docker /opt/homebrew/bin/docker; do
  if "$cmd" info &>/dev/null 2>&1; then
    DOCKER_CMD="$cmd"
    break
  fi
done

if [ -z "$DOCKER_CMD" ]; then
  log "Docker not running, attempting to start..."
  open -a Docker
  for i in $(seq 1 30); do
    sleep 2
    for cmd in /usr/local/bin/docker /opt/homebrew/bin/docker; do
      if "$cmd" info &>/dev/null 2>&1; then
        DOCKER_CMD="$cmd"
        break 2
      fi
    done
  done
fi

if [ -z "$DOCKER_CMD" ]; then
  log "ERROR: Docker failed to start after 60s"
  exit 1
fi

log "Docker ready, starting services..."
cd "$PROJECT_DIR"
"$DOCKER_CMD" compose up -d 2>> "$LOG_FILE"
log "=== Services started. Worker will backfill + collect automatically. ==="
