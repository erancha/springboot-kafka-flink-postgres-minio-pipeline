#!/usr/bin/env bash
# Start the stack, or rebuild/recreate only the named services.
# Usage: start.sh [--restart] [--rebuild] [--profile <name>] [service...]
#   --restart          Full stack: stop first. With services: --force-recreate them.
#   --rebuild          Rebuild Docker images before starting
#   --profile testing  Apply docker-compose.testing.yml (enables www.gstatic.com image URL fetching)
#   service...         Limit the operation to named services, e.g. 'start.sh --rebuild backend'
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

require_docker

# Rebuild/recreate only the named services. Two of them need extra handling:
#   ui        - its build context cannot read ../backend, so stage the shared
#               event-payload schema into frontend/public before building.
#   flink-job - it submits the job then idles; the JobManager keeps the prior
#               submission running across a plain recreate, so recycle the
#               JobManager/TaskManager (no persisted state) to clear the old job
#               before the rebuilt job is resubmitted.
start_services() {
  local -a targets=("$@")
  local svc
  local recycle_cluster=false

  for svc in "$@"; do
    case "$svc" in
      ui)
        mkdir -p "$ROOT_DIR/frontend/public"
        cp "$ROOT_DIR/backend/src/main/resources/event-payload-schema.json" \
           "$ROOT_DIR/frontend/public/event-payload-schema.json"
        ;;
      flink-job) recycle_cluster=true ;;
    esac
  done

  if $recycle_cluster; then
    targets=(flink-jobmanager flink-taskmanager "${targets[@]}")
    RESTART=true
  fi

  local -a args=(up -d)
  if $REBUILD; then args+=(--build); fi
  if $RESTART; then args+=(--force-recreate); fi

  compose "${args[@]}" "${targets[@]}"
  compose ps
}

RESTART=false
REBUILD=false
SERVICES=()
while [[ $# -gt 0 ]]; do
  case "$1" in
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

if [[ ${#SERVICES[@]} -gt 0 ]]; then
  start_services "${SERVICES[@]}"
  exit 0
fi

if $RESTART; then
  compose down
fi

if $REBUILD; then
  compose up -d --build
else
  compose up -d
fi
compose ps
