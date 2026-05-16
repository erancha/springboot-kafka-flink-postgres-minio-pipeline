#!/usr/bin/env bash
# Start the stack.
# Usage: start.sh [--restart] [--rebuild] [--profile <name>]
#   --restart          Stop the stack first before starting
#   --rebuild          Rebuild all Docker images before starting
#   --profile testing  Apply docker-compose.testing.yml (enables picsum.photos image URL fetching)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

require_docker

RESTART=false
REBUILD=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --restart) RESTART=true ;;
    --rebuild) REBUILD=true ;;
    --profile)
      PROFILE="${2:-}"
      shift
      OVERRIDE_FILE="$ROOT_DIR/docker-compose.${PROFILE}.yml"
      if [[ ! -f "$OVERRIDE_FILE" ]]; then
        echo "Unknown profile '$PROFILE': $OVERRIDE_FILE not found" >&2
        exit 1
      fi
      export COMPOSE_OVERRIDE="$OVERRIDE_FILE"
      ;;
  esac
  shift
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
