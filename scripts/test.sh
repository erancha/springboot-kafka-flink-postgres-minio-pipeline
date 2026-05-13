#!/usr/bin/env bash
# Run the project test suites. Default: all suites including Testcontainers IT.
# Usage: test.sh [suite...]
#   Suites: backend  flink  flink-it  frontend
#   backend   – Spring Boot unit tests (no Docker)
#   flink     – Flink unit tests (no Docker)
#   flink-it  – Flink unit + Testcontainers integration tests (requires Docker)
#   frontend  – Vitest frontend tests (no Docker)
# Examples:
#   ./scripts/test.sh                    # run all suites
#   ./scripts/test.sh backend frontend   # run only backend and frontend
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── helpers ──────────────────────────────────────────────────────────────────

PASS=0
FAIL=0
ERRORS=()

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
  # clean ensures IDE-compiled .class files with wrong source level don't fool Maven's incremental check
  run_suite "backend (unit)" mvn -f "$ROOT_DIR/backend/pom.xml" clean test
}

suite_flink() {
  run_suite "flink (unit)" mvn -f "$ROOT_DIR/flink/pom.xml" clean test
}

suite_flink_it() {
  # shellcheck source=common.sh
  source "$SCRIPT_DIR/common.sh"
  require_docker
  run_suite "flink-it (unit + integration)" mvn -f "$ROOT_DIR/flink/pom.xml" clean verify
}

suite_frontend() {
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
