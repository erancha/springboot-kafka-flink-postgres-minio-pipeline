#!/usr/bin/env bash
# Start the stack, or rebuild/recreate only the named services.
# Usage: start.sh [--fresh] [--restart] [--rebuild] [--profile <name>] [service...]
#   --fresh            Full stack: stop, wipe volumes (DESTROYS Postgres/Kafka/MinIO data) and prune
#                      this project's dangling images, then start. Does NOT rebuild images — pair
#                      with --rebuild (e.g. 'start.sh --fresh --rebuild'). Not combinable with
#                      --restart or a service list.
#   --restart          Full stack: stop first (keeps volumes). With services: --force-recreate them.
#   --rebuild          Rebuild Docker images before starting
#   --profile testing  Apply docker-compose.testing.yml (enables www.gstatic.com image URL fetching)
#   service...         Limit the operation to named services, e.g. 'start.sh --rebuild backend'
#
# Thin front-end: every docker action goes through docker-helper.sh, which owns the stack lifecycle
# (up / stop / build / prune). This script only parses flags and delegates.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

helper() { "$SCRIPT_DIR/docker-helper.sh" "$@"; }

FRESH=false
RESTART=false
REBUILD=false
SERVICES=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fresh) FRESH=true ;;
    --restart) RESTART=true ;;
    --rebuild) REBUILD=true ;;
    --profile)
      PROFILE="${2:-}"
      shift
      OVERRIDE_FILE="$ROOT_DIR/scripts/docker-compose.${PROFILE}.yml"
      if [[ ! -f "$OVERRIDE_FILE" ]]; then
        echo "Unknown profile '$PROFILE': $OVERRIDE_FILE not found" >&2
        exit 1
      fi
      export COMPOSE_OVERRIDE="$OVERRIDE_FILE"
      ;;
    -*) echo "Unknown option '$1'" >&2; exit 1 ;;
    *) SERVICES+=("$1") ;;
  esac
  shift
done

if $FRESH; then
  if $RESTART; then
    echo "--fresh and --restart both stop the stack but differ on volumes; pick one." >&2
    exit 1
  fi
  if [[ ${#SERVICES[@]} -gt 0 ]]; then
    echo "--fresh operates on the whole stack; drop the service list." >&2
    exit 1
  fi
fi

# Service-scoped: recreate/build just those, no whole-stack stop. docker-helper.sh --up handles the
# flink-job cluster recycle when flink-job is among them.
if [[ ${#SERVICES[@]} -gt 0 ]]; then
  up=(--up)
  if $REBUILD; then up+=(--build); fi
  if $RESTART; then up+=(--recreate); fi
  helper "${up[@]}" "${SERVICES[@]}"
  exit 0
fi

# Whole stack: the optional stop is delegated (--fresh wipes + prunes; --restart keeps volumes),
# then bring everything up.
if $FRESH; then
  helper --stop --prune
elif $RESTART; then
  helper --stop --keep-volumes
fi

up=(--up)
if $REBUILD; then up+=(--build); fi
helper "${up[@]}"
