#!/usr/bin/env bash
# Inspect the images bucket via a one-off AWS CLI container (the stack's S3 client).
# Usage: object-store-helper.sh ls [prefix]       list objects (optionally under a key prefix)
#        object-store-helper.sh cat <object-key>  stream an object to stdout
# Examples:
#   ./scripts/object-store-helper.sh ls
#   ./scripts/object-store-helper.sh ls images/2026-04-15/
#   ./scripts/object-store-helper.sh cat images/2026-04-15/<uuid>.jpg
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
    S3_CMD="aws s3 ls \"s3://\$OBJECT_STORE_BUCKET/$PREFIX\""
    ;;
  cat)
    KEY="${2:-}"
    if [[ -z "$KEY" ]]; then
      echo "Usage: object-store-helper.sh cat <object-key>" >&2
      exit 1
    fi
    S3_CMD="aws s3 cp \"s3://\$OBJECT_STORE_BUCKET/$KEY\" -"
    ;;
  *)
    echo "Usage: object-store-helper.sh ls [prefix] | cat <object-key>" >&2
    exit 1
    ;;
esac

s3_cli "$S3_CMD"
