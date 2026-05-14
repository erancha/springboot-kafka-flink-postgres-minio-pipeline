#!/usr/bin/env bash
# Stop the stack.
# Usage: down.sh [--remove-volumes [--prune-dangling-images]]
#   --remove-volumes         Also remove volumes (data lost)
#   --prune-dangling-images  With --remove-volumes: also purge dangling Docker images and volumes from builds
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,5p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

require_docker

if [[ "${1:-}" == "--remove-volumes" ]]; then
  compose down -v --remove-orphans
  if [[ "${2:-}" == "--prune-dangling-images" ]]; then
    docker image prune -f
    docker volume prune -f
  fi
else
  compose down
fi
