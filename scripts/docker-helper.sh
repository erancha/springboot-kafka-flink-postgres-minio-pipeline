#!/usr/bin/env bash
# Docker Compose helper for the local stack. One entry point for the thin compose ops.
# Usage: docker-helper.sh <command> [args]
#   --build
#       Build all stack images.
#   --stop [--keep-volumes] [--prune-dangling-images]
#       Stop the stack. Default removes volumes; --keep-volumes preserves data.
#       --prune-dangling-images also purges dangling images/volumes (ignored with --keep-volumes).
#   --logs [-e|--errors] [service]
#       Stream logs (follow, last 200 lines). -e filters to WARN/ERROR/EXCEPTION/FATAL;
#       service limits output to one service (default: all).
#   -h, --help
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || -z "${1:-}" ]]; then
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  [[ -z "${1:-}" ]] && exit 1
  exit 0
fi

# Builds all images. The frontend build context excludes ../backend, so the payload
# schema is staged into frontend/public where the frontend image can reach it.
do_build() {
  mkdir -p "$ROOT_DIR/frontend/public"
  cp "$ROOT_DIR/backend/src/main/resources/event-payload-schema.json" \
     "$ROOT_DIR/frontend/public/event-payload-schema.json"
  compose build
}

do_stop() {
  if [[ "${1:-}" == "--keep-volumes" ]]; then
    compose down
  else
    compose down -v --remove-orphans
    if [[ "${1:-}" == "--prune-dangling-images" ]]; then
      docker image prune -f
      docker volume prune -f
    fi
  fi
}

do_logs() {
  local only_errors=false
  if [[ "${1:-}" == "--errors" || "${1:-}" == "-e" ]]; then
    only_errors=true
    shift
  fi
  if [[ "$only_errors" == "true" ]]; then
    compose logs -f --tail=200 "$@" | grep -Eai '(warn|warning|error|exception|fatal)'
  else
    compose logs -f --tail=200 "$@"
  fi
}

require_docker

mode="$1"
shift
case "$mode" in
  --build) do_build "$@" ;;
  --stop)  do_stop "$@" ;;
  --logs)  do_logs "$@" ;;
  *) echo "Unknown command: $mode (expected --build | --stop | --logs; see -h)" >&2; exit 1 ;;
esac
