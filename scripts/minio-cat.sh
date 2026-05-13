#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_docker

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <object-key>" >&2
  echo "Example: $0 images/2026-04-15/<uuid>.jpg" >&2
  exit 1
fi

KEY="$1"

compose run --rm minio-init /bin/sh -lc "mc alias set local http://minio:9000 minio minio123 >/dev/null && mc cat local/images/$KEY"
