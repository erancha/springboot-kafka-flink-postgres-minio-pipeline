#!/usr/bin/env bash
# Inspect the MinIO images bucket via a one-off mc container.
# Usage: minio-helper.sh ls [prefix]       list objects (optionally under a key prefix)
#        minio-helper.sh cat <object-key>  stream an object to stdout
# Examples:
#   ./scripts/minio-helper.sh ls
#   ./scripts/minio-helper.sh ls images/2026-04-15/
#   ./scripts/minio-helper.sh cat images/2026-04-15/<uuid>.jpg
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

require_docker

case "${1:-}" in
  ls)
    PREFIX="${2:-}"
    [[ -n "$PREFIX" ]] && PREFIX="/$PREFIX"
    MC_CMD="mc ls local/images$PREFIX"
    ;;
  cat)
    KEY="${2:-}"
    if [[ -z "$KEY" ]]; then
      echo "Usage: minio-helper.sh cat <object-key>" >&2
      exit 1
    fi
    MC_CMD="mc cat local/images/$KEY"
    ;;
  *)
    echo "Usage: minio-helper.sh ls [prefix] | cat <object-key>" >&2
    exit 1
    ;;
esac

# --entrypoint overrides minio-init's bucket-bootstrap entrypoint; without it the mc
# command below would only be passed as ignored args to that entrypoint and never run.
compose run --rm --entrypoint /bin/sh minio-init \
  -lc "mc alias set local http://minio:9000 minio minio123 >/dev/null && $MC_CMD"
