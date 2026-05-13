#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_docker

PREFIX="${1:-}"

if [[ -n "$PREFIX" ]]; then
  PREFIX="/$PREFIX"
fi

compose run --rm minio-init /bin/sh -lc "mc alias set local http://minio:9000 minio minio123 >/dev/null && mc ls local/images$PREFIX"
