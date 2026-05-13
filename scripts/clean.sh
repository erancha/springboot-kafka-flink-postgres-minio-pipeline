#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_docker

compose down -v --remove-orphans

# Optional: remove any dangling images and volumes from builds
if [[ "${1:-}" == "--prune" ]]; then
  docker image prune -f
  docker volume prune -f
fi
