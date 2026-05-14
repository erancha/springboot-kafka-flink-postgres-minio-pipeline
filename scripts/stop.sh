#!/usr/bin/env bash
# Stop the stack.
# Usage: stop.sh [--keep-volumes [--prune-dangling-images]]
#   --keep-volumes           Keep volumes (data preserved)
#   --prune-dangling-images  Without --keep-volumes: also purge dangling Docker images and volumes from builds
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,5p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

require_docker

if [[ "${1:-}" == "--keep-volumes" ]]; then
  compose down
else
  compose down -v --remove-orphans
  if [[ "${1:-}" == "--prune-dangling-images" ]]; then
    docker image prune -f
    docker volume prune -f
  fi
fi
