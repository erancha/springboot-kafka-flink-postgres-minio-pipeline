#!/usr/bin/env bash
# Run the project test suites. Default: all suites including Testcontainers IT.
# Usage: test.sh [suite...]
#   Suites: backend  flink  flink-it  frontend
#   backend   – Spring Boot unit tests (no Docker)
#   flink     – Flink unit tests (no Docker)
#   flink-it  – Flink unit + Testcontainers integration tests (requires Docker)
#   frontend  – Vitest frontend unit tests (no Docker)
# Examples:
#   ./scripts/test.sh                    # run all suites
#   ./scripts/test.sh backend frontend   # run only backend and frontend
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# ── helpers ──────────────────────────────────────────────────────────────────

PASS=0
FAIL=0
ERRORS=()

# Install frontend dependencies on demand so a fresh clone is testable without a manual npm step.
# npm ci is reproducible against the committed package-lock.json.
ensure_frontend_deps() {
  [[ -x "$ROOT_DIR/frontend/node_modules/.bin/vitest" ]] && return 0
  echo "Installing frontend dependencies (npm ci) …"
  ( cd "$ROOT_DIR/frontend" && npm ci )
}

# Pre-pull the Testcontainers images the integration tests need, so a transient Docker registry
# hiccup (mid-download EOF) is retried here instead of failing the IT build on a fresh host.
# Best-effort: a failed pre-pull only warns; the test itself still pulls anything missing.
prepull_it_images() {
  local image attempt
  for image in postgres:16 "${TESTCONTAINERS_RYUK_IMAGE:-testcontainers/ryuk:0.7.0}"; do
    for attempt in 1 2 3; do
      docker pull "$image" && break
      if [[ $attempt -eq 3 ]]; then
        echo "  warning: could not pre-pull $image after 3 attempts; the test will retry." >&2
      else
        echo "  pull $image failed (attempt $attempt/3); retrying …" >&2
        sleep 3
      fi
    done
  done
}

run_suite() {
  local name="$1"; shift
  echo ""
  echo "━━━  $name  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if "$@"; then
    echo "✓  $name passed"
    PASS=$((PASS + 1))
  else
    echo "✗  $name FAILED"
    FAIL=$((FAIL + 1))
    ERRORS+=("$name")
  fi
}

suite_backend() {
  ensure_java21
  # clean ensures IDE-compiled .class files with wrong source level don't fool Maven's incremental check
  run_suite "backend (unit)" mvn -f "$ROOT_DIR/backend/pom.xml" clean test
}

suite_flink() {
  ensure_java21
  run_suite "flink (unit)" mvn -f "$ROOT_DIR/flink/pom.xml" clean test
}

suite_flink_it() {
  ensure_java21
  require_docker
  prepull_it_images
  run_suite "flink-it (unit + integration)" mvn -f "$ROOT_DIR/flink/pom.xml" clean verify
}

suite_frontend() {
  ensure_frontend_deps
  run_suite "frontend (unit)" bash -c "cd '$ROOT_DIR/frontend' && npm test -- --run"
}

# ── argument parsing ──────────────────────────────────────────────────────────

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

SUITES=("$@")

if [[ ${#SUITES[@]} -eq 0 ]]; then
  SUITES=(backend flink-it frontend)
fi

for suite in "${SUITES[@]}"; do
  case "$suite" in
    backend)   suite_backend ;;
    flink)     suite_flink ;;
    flink-it)  suite_flink_it ;;
    frontend)  suite_frontend ;;
    *)
      echo "Unknown suite: '$suite'. Valid: backend flink flink-it frontend" >&2
      exit 1
      ;;
  esac
done

# ── summary ───────────────────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo "  Results: ${PASS} passed, ${FAIL} failed"
if [[ ${#ERRORS[@]} -gt 0 ]]; then
  for e in "${ERRORS[@]}"; do echo "    ✗  $e"; done
  echo "══════════════════════════════════════════════════════════════════════"
  exit 1
fi
echo "══════════════════════════════════════════════════════════════════════"
