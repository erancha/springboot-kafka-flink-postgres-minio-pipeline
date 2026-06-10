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
  # Names the host in Flink alerts; bash's $HOSTNAME is not exported to compose.
  export MACHINE_NAME="${MACHINE_NAME:-$(hostname)}"
  (cd "$ROOT_DIR" && docker compose --project-directory "$ROOT_DIR" "${files[@]}" "$@")
}

# The frontend build context excludes ../backend, so the frontend image cannot reach the schema at
# its source. Stage it into frontend/public (Vite bundles public/* into the served root) before any
# frontend build, or the UI's example pre-fill fetches 404 and the empty {} default is submitted.
stage_payload_schema() {
  mkdir -p "$ROOT_DIR/frontend/public"
  cp "$ROOT_DIR/backend/src/main/resources/event-payload-schema.json" \
     "$ROOT_DIR/frontend/public/event-payload-schema.json"
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

# This repo targets Java 21. The host default JDK may be newer (e.g. 25), which both trips the
# backend's maven-enforcer rule and breaks Mockito's inline mock-maker on JDK system classes. Point
# Maven/java at a JDK 21 by discovery rather than a hardcoded path, so it works across machines.
ensure_java21() {
  if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]] \
      && "${JAVA_HOME}/bin/java" -version 2>&1 | grep -q 'version "21'; then
    return 0
  fi
  local home
  for home in /usr/lib/jvm/java-21-openjdk-* /usr/lib/jvm/temurin-21-* /usr/lib/jvm/jdk-21* \
              /usr/lib/jvm/*-21-* "${HOME}"/.sdkman/candidates/java/21*; do
    if [[ -x "${home}/bin/java" ]] && "${home}/bin/java" -version 2>&1 | grep -q 'version "21'; then
      export JAVA_HOME="${home}"
      export PATH="${home}/bin:${PATH}"
      return 0
    fi
  done
  echo "Java 21 is required but was not found." >&2
  echo "Install it (e.g. sudo apt-get install -y openjdk-21-jdk-headless) or set JAVA_HOME to a JDK 21." >&2
  exit 1
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
