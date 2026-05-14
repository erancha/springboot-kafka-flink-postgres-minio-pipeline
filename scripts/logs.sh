#!/usr/bin/env bash
# Stream Docker Compose logs.
# Usage: logs.sh [-e|--errors] [service]
#   -e, --errors  Filter output to lines containing WARN/ERROR/EXCEPTION/FATAL
#   service       Stream logs for a specific service only (default: all)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,5p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

require_docker

only_errors=false
if [[ ${1:-} == "--errors" || ${1:-} == "-e" ]]; then
  only_errors=true
  shift
fi

if [[ $# -ge 1 ]]; then
  if [[ "$only_errors" == "true" ]]; then
    compose logs -f --tail=200 "$@" | grep -Eai '(warn|warning|error|exception|fatal)'
  else
    compose logs -f --tail=200 "$@"
  fi
else
  if [[ "$only_errors" == "true" ]]; then
    compose logs -f --tail=200 | grep -Eai '(warn|warning|error|exception|fatal)'
  else
    compose logs -f --tail=200
  fi
fi
