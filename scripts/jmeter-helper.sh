#!/usr/bin/env bash
# Load-test the backend's POST /api/events ingestion path with Apache JMeter.
# Drives the jmeter/backend-load.jmx plan, which floods the endpoint with unique
# DATA events and asserts a 2xx per request. Use send-event.sh for single-shot,
# end-to-end (Flink + MinIO) checks; this tool is purely about ingestion throughput.
# Usage: jmeter-helper.sh [options]
#   -t, --threads N      concurrent virtual users   (default 10)
#   -i, --iterations N   requests per thread         (default 10)
#       --skip-preflight skip the send-event.sh pipeline check; flood the backend
#                        even when Flink/MinIO downstream is unhealthy (edge case)
#   -h, --help
# Total requests sent = threads x iterations.
# Examples:
#   ./scripts/jmeter-helper.sh                     # 10 threads x 10 = 100 requests
#   ./scripts/jmeter-helper.sh -t 50 -i 20         # 50 threads x 20 = 1000 requests
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

JMETER_VERSION=5.6.3
JMETER_HOME_DIR="$ROOT_DIR/jmeter"
RUNTIME_DIR="$JMETER_HOME_DIR/.runtime"
RESULTS_DIR="$JMETER_HOME_DIR/results"
TEST_PLAN="$JMETER_HOME_DIR/backend-load.jmx"
DOWNLOAD_BASE="https://archive.apache.org/dist/jmeter/binaries"

THREADS=10
ITERATIONS=10
SKIP_PREFLIGHT=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--threads)     THREADS="${2:-}"; shift 2 ;;
    -i|--iterations)  ITERATIONS="${2:-}"; shift 2 ;;
    --skip-preflight) SKIP_PREFLIGHT=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

for pair in "threads:$THREADS" "iterations:$ITERATIONS"; do
  if [[ ! "${pair#*:}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${pair%%:*} must be a positive integer (got: '${pair#*:}')" >&2
    exit 1
  fi
done

# Resolve a runnable jmeter, honoring an already-installed one before downloading.
# A missing system install falls back to a version-pinned, no-sudo vendored copy
# under jmeter/.runtime so a fresh clone can load-test without manual setup.
resolve_jmeter() {
  if command -v jmeter >/dev/null 2>&1; then
    JMETER_BIN="$(command -v jmeter)"
    return 0
  fi

  command -v java >/dev/null 2>&1 || {
    echo "Java is required to run JMeter but was not found on PATH." >&2
    exit 1
  }

  local dir="$RUNTIME_DIR/apache-jmeter-$JMETER_VERSION"
  JMETER_BIN="$dir/bin/jmeter"
  [[ -x "$JMETER_BIN" ]] && return 0

  command -v curl >/dev/null 2>&1 || { echo "curl is required to download JMeter." >&2; exit 1; }
  command -v tar  >/dev/null 2>&1 || { echo "tar is required to extract JMeter." >&2; exit 1; }

  local tgz="apache-jmeter-$JMETER_VERSION.tgz"
  local tmp; tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN

  echo "JMeter not found; downloading Apache JMeter $JMETER_VERSION (one-time) …"
  curl -fsSL -o "$tmp/$tgz" "$DOWNLOAD_BASE/$tgz"

  # Fail closed on a checksum mismatch; only a checksum we cannot fetch is tolerated,
  # so a transient outage of the sums endpoint doesn't block an otherwise-good download.
  local expected actual
  expected="$(curl -fsSL "$DOWNLOAD_BASE/$tgz.sha512" 2>/dev/null | grep -oiE '[0-9a-f]{128}' | head -1 || true)"
  if [[ -n "$expected" ]]; then
    actual="$(sha512sum "$tmp/$tgz" | awk '{print $1}')"
    if [[ "$expected" != "$actual" ]]; then
      echo "Checksum mismatch for $tgz — refusing to use the download." >&2
      exit 1
    fi
  else
    echo "  warning: could not fetch published checksum; skipping integrity verification." >&2
  fi

  mkdir -p "$RUNTIME_DIR"
  tar -xzf "$tmp/$tgz" -C "$RUNTIME_DIR"
  [[ -x "$JMETER_BIN" ]] || { echo "JMeter extraction did not yield $JMETER_BIN" >&2; exit 1; }
}

load_env
: "${BACKEND_PORT:?BACKEND_PORT not set — is .env present? See .env.example}"
BACKEND_URL="http://localhost:${BACKEND_PORT}/api/events"

[[ -f "$TEST_PLAN" ]] || { echo "Test plan not found: $TEST_PLAN" >&2; exit 1; }

resolve_jmeter

# Preflight via send-event.sh (its default exercises both DATA and IMAGE end-to-end),
# so a run never floods a backend whose downstream is broken. --skip-preflight bypasses
# this to deliberately load an ingestion path whose downstream (Flink/MinIO) is unhealthy.
if [[ "$SKIP_PREFLIGHT" == true ]]; then
  echo "→ Preflight skipped (--skip-preflight); loading the backend without a pipeline check."
elif ! "$SCRIPT_DIR/send-event.sh"; then
  echo "✗ Preflight failed — not starting the load run. See output above." >&2
  echo "  Re-run with --skip-preflight to load the backend anyway." >&2
  exit 1
else
  echo "✓ Preflight passed."
fi

ts="$(date +%Y%m%d-%H%M%S)"
jtl="$RESULTS_DIR/$ts.jtl"
report="$RESULTS_DIR/$ts-report"
mkdir -p "$RESULTS_DIR"

echo
echo "→ Load run: ${THREADS} threads x ${ITERATIONS} iterations = $((THREADS * ITERATIONS)) requests → ${BACKEND_URL}"
"$JMETER_BIN" -n -t "$TEST_PLAN" \
  -Jthreads="$THREADS" -Jiterations="$ITERATIONS" -Jbackend_url="$BACKEND_URL" \
  -l "$jtl" -j "$RESULTS_DIR/$ts.log" -e -o "$report"

echo
echo "✓ Done. Raw results: ${jtl}"
echo "  HTML dashboard:    ${report}/index.html"
