#!/usr/bin/env bash
# Run psql against the warehouse database in the postgres container.
# Usage: sql-helper.sh [psql-args...]         interactive shell, or e.g. -c "SELECT 1"
#        sql-helper.sh -f <file> [psql-args]  run a host SQL file (piped via stdin)
#        sql-helper.sh --host <name|ip> ...   run the container's psql against a remote host
#                                             instead of the local DB; PGPASSWORD comes from env/.env
# Examples:
#   ./scripts/sql-helper.sh
#   ./scripts/sql-helper.sh -tA -c "SELECT count(*) FROM processed_events;"
#   ./scripts/sql-helper.sh -c "TRUNCATE processed_events, event_type_counts_agg, image_size_buckets_agg RESTART IDENTITY;"
#   ./scripts/sql-helper.sh -f infra/postgres/analytics.sql
#   ./scripts/sql-helper.sh --host db.example.com -c "SELECT 1"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

# --host is ours, not psql's: strip it so every other arg forwards to psql untouched.
REMOTE_HOST=""
ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) REMOTE_HOST="${2:?--host needs a name or ip}"; shift 2 ;;
    *)      ARGS+=("$1"); shift ;;
  esac
done
set -- "${ARGS[@]+"${ARGS[@]}"}"

require_docker

# With --host the container is only a psql client dialing a remote server: it must be up, and the
# stack's POSTGRES_PASSWORD authenticates by default. Without --host, psql uses the local socket.
HOST_ARGS=()
ENV_ARGS=()
if [[ -n "$REMOTE_HOST" ]]; then
  load_env
  compose up -d postgres
  HOST_ARGS=(-h "$REMOTE_HOST")
  ENV_ARGS=(-e "PGPASSWORD=${PGPASSWORD:-${POSTGRES_PASSWORD:-}}")
fi

PSQL=(psql "${HOST_ARGS[@]+"${HOST_ARGS[@]}"}" -U postgres -d warehouse)

# -f names a file on the host, not inside the container, so it is streamed in via
# stdin rather than psql's own -f (which would resolve the path container-side).
if [[ "${1:-}" == "-f" || "${1:-}" == "--file" ]]; then
  SQL_FILE="${2:-}"
  if [[ -z "$SQL_FILE" || ! -f "$SQL_FILE" ]]; then
    echo "SQL file not found: ${SQL_FILE:-<none given>}" >&2
    exit 1
  fi
  shift 2
  compose exec -T "${ENV_ARGS[@]+"${ENV_ARGS[@]}"}" postgres "${PSQL[@]}" "$@" < "$SQL_FILE"
else
  compose exec -T "${ENV_ARGS[@]+"${ENV_ARGS[@]}"}" postgres "${PSQL[@]}" "$@"
fi
