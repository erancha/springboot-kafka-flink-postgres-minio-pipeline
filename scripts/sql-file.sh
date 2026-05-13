#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_docker

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path-to-sql-file>" >&2
  exit 1
fi

SQL_FILE="$1"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "SQL file not found: $SQL_FILE" >&2
  exit 1
fi

compose exec -T postgres psql -U postgres -d warehouse < "$SQL_FILE"
