#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_docker

# The frontend image build context excludes ../backend, so stage the schema where it can reach it.
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
mkdir -p "$ROOT_DIR/frontend/public"
cp "$ROOT_DIR/backend/src/main/resources/event-payload-schema.json" \
   "$ROOT_DIR/frontend/public/event-payload-schema.json"

compose build
