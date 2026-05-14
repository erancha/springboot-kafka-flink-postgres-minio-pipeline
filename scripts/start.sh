#!/usr/bin/env bash
# Start the stack.
# Usage: start.sh [--restart] [--rebuild]
#   --restart  Stop the stack first before starting
#   --rebuild  Rebuild all Docker images before starting
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,5p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

require_docker

RESTART=false
REBUILD=false
for arg in "$@"; do
  case "$arg" in
    --restart) RESTART=true ;;
    --rebuild) REBUILD=true ;;
  esac
done

if $RESTART; then
  compose down
fi

if $REBUILD; then
  compose up -d --build
else
  compose up -d
fi
compose ps
