#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

compose() {
  local -a files=(-f "$ROOT_DIR/docker-compose.yml")
  [[ -n "${COMPOSE_OVERRIDE:-}" ]] && files+=(-f "$COMPOSE_OVERRIDE")
  (cd "$ROOT_DIR" && docker compose "${files[@]}" "$@")
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
