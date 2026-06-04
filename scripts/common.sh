#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The compose files live in scripts/, but their bind-mount sources and build
# contexts (./infra, ./backend, ./flink, ./frontend) are written relative to the
# repo root. --project-directory pins that resolution base to ROOT_DIR so those
# paths stay valid regardless of where the compose file sits.
compose() {
  local -a files=(-f "$ROOT_DIR/scripts/docker-compose.yml")
  [[ -n "${COMPOSE_OVERRIDE:-}" ]] && files+=(-f "$COMPOSE_OVERRIDE")
  (cd "$ROOT_DIR" && docker compose --project-directory "$ROOT_DIR" "${files[@]}" "$@")
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

# Source .env into the environment for scripts that need values in the shell.
# docker compose reads .env itself, so only shell-side scripts call this.
# No-op if .env is absent.
load_env() {
  if [[ -f "$ROOT_DIR/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$ROOT_DIR/.env"
    set +a
  fi
}
