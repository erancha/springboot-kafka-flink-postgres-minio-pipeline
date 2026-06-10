#!/usr/bin/env bash
# Run psql against the warehouse database in the postgres container.
# Usage: sql-helper.sh [psql-args...]        interactive shell, or e.g. -c "SELECT 1"
#        sql-helper.sh -f <file> [psql-args] run a host SQL file (piped via stdin)
# Examples:
#   ./scripts/sql-helper.sh
#   ./scripts/sql-helper.sh -tA -c "SELECT count(*) FROM processed_events;"
#   ./scripts/sql-helper.sh -c "TRUNCATE processed_events, event_type_counts_agg, image_size_buckets_agg RESTART IDENTITY;"
#   ./scripts/sql-helper.sh -f infra/postgres/analytics.sql
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

require_docker

# -f names a file on the host, not inside the container, so it is streamed in via
# stdin rather than psql's own -f (which would resolve the path container-side).
if [[ "${1:-}" == "-f" || "${1:-}" == "--file" ]]; then
  SQL_FILE="${2:-}"
  if [[ -z "$SQL_FILE" || ! -f "$SQL_FILE" ]]; then
    echo "SQL file not found: ${SQL_FILE:-<none given>}" >&2
    exit 1
  fi
  shift 2
  compose exec -T postgres psql -U postgres -d warehouse "$@" < "$SQL_FILE"
else
  compose exec -T postgres psql -U postgres -d warehouse "$@"
fi
