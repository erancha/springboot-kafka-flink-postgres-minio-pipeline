#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

compose() {
  (cd "$ROOT_DIR" && docker compose "$@")
}

require_docker() {
  command -v docker >/dev/null 2>&1 || {
    echo "docker not found" >&2
    exit 1
  }

  docker info >/dev/null 2>&1 || {
    echo "docker daemon not reachable (is Docker Desktop running?)" >&2
    exit 1
  }
}
