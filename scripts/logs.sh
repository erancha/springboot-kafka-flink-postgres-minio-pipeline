#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

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
