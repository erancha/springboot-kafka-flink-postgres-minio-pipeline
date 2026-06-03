#!/usr/bin/env bash
# Generate concurrent DATA/IMAGE-url load against the running backend to
# exercise the Kafka->Flink->(MinIO,Postgres) pipeline under volume.
# Pure load generation: reports HTTP-level results; asserts nothing about
# the pipeline. Verify landing via Grafana / Kafka UI / compare-images.sh.
# Usage: stress.sh [options]
#   -t, --threads N      concurrent workers          (default 4)
#   -i, --iterations N   requests per worker per type (default 100)
#   -y, --type TYPE      data | image | both          (default both)
#       --reset-and-verify-counts  TRUNCATE all warehouse tables, then verify the
#                        2xx sends landed in processed_events
#   -h, --help
# Examples:
#   ./scripts/stress.sh -t 8 -i 500 --type data
#   ./scripts/stress.sh --type both
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

THREADS=4
ITERATIONS=100
TYPE=both
RESET_AND_VERIFY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--threads)    THREADS="${2:-}"; shift 2 ;;
    -i|--iterations) ITERATIONS="${2:-}"; shift 2 ;;
    -y|--type)       TYPE="${2:-}"; shift 2 ;;
    --reset-and-verify-counts) RESET_AND_VERIFY=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if ! [[ "$THREADS" =~ ^[1-9][0-9]*$ ]]; then
  echo "threads must be a positive integer (got: '$THREADS')" >&2
  exit 1
fi
if ! [[ "$ITERATIONS" =~ ^[1-9][0-9]*$ ]]; then
  echo "iterations must be a positive integer (got: '$ITERATIONS')" >&2
  exit 1
fi
case "$TYPE" in
  data|image|both) ;;
  *) echo "type must be one of: data image both (got: '$TYPE')" >&2; exit 1 ;;
esac

load_env
: "${BACKEND_PORT:?BACKEND_PORT not set — is .env present? See .env.example}"
BASE_URL="http://localhost:${BACKEND_PORT}"
EVENTS_URL="${BASE_URL}/api/events"
IMAGE_BODY='{"eventType":"IMAGE","imageUrl":"https://www.gstatic.com/webp/gallery/1.jpg"}'
MAX_TIME=10
VERIFY_TIMEOUT_SECS=60

RUN_DIR="$(mktemp -d)"
CANARY_BODY_FILE="$(mktemp)"
trap 'rm -rf "$RUN_DIR" "$CANARY_BODY_FILE"' EXIT

RUN_START=0
RUN_END=0
BASELINE_COUNT=""
TOTAL_OK=0

# Computes the per-run total request count from THREADS/ITERATIONS/TYPE.
total_requests() {
  local per_iter=1
  [[ "$TYPE" == "both" ]] && per_iter=2
  echo $((THREADS * ITERATIONS * per_iter))
}

# Prints the run configuration before load starts.
print_banner() {
  echo "─────────────────────────────────────────────"
  echo "  stress: threads=${THREADS} iterations=${ITERATIONS} type=${TYPE}"
  echo "  target: ${EVENTS_URL}"
  echo "  total:  $(total_requests) requests"
  echo "─────────────────────────────────────────────"
}

# Builds a DATA event JSON body. Args: worker-id, iteration, epoch-seconds.
data_body() {
  printf '{"eventType":"DATA","payload":{"thread":%d,"iter":%d,"ts":%d}}' \
    "$1" "$2" "$3"
}

# POSTs $1 as JSON to EVENTS_URL, writing the response body to file $2
# (use /dev/null to discard). Echoes the HTTP status code, or 000 on a
# transport failure (connection refused, timeout). Never aborts.
http_post() {
  local body="$1" out="$2" code
  code="$(curl -s -o "$out" -w '%{http_code}' --max-time "$MAX_TIME" \
          -H 'Content-Type: application/json' \
          -X POST -d "$body" "$EVENTS_URL" 2>/dev/null)" || code="000"
  printf '%s' "$code"
}

# Echoes the current processed_events row count via scripts/sql.sh (psql in the
# postgres container). Empty string if the query or Docker is unavailable;
# never aborts the script.
db_processed_count() {
  local out
  out="$("$SCRIPT_DIR/sql.sh" -tA -c "SELECT count(*) FROM processed_events;" 2>/dev/null \
        | tr -d '[:space:]')" || true
  printf '%s' "$out"
}

# Echoes how many canary events preflight publishes: always 1 DATA, plus 1
# IMAGE when the type includes image. Canaries land in processed_events too,
# so the verify expectation must account for them.
canary_count() {
  if [[ "$TYPE" == "data" ]]; then echo 1; else echo 2; fi
}

# Truncates every warehouse table so the verify baseline starts from zero.
# Aborts the script if the reset cannot be performed: a skipped or partial
# reset would silently invalidate the count verification. Runs before the
# load, when the tables are quiescent, so it does not contend with writers.
reset_tables() {
  echo "⚠ resetting warehouse tables (processed_events, event_type_counts_99m, image_size_buckets_99m)…"
  if ! "$SCRIPT_DIR/sql.sh" -q -c \
      "TRUNCATE processed_events, event_type_counts_99m, image_size_buckets_99m;" >/dev/null 2>&1; then
    echo "✗ table reset failed — is Postgres/Docker up? Aborting." >&2
    exit 1
  fi
}

# Live-canary preflight: one DATA canary (and one IMAGE canary when the type
# includes image) against the running backend. Aborts the script loudly
# exactly once on any failure, before any worker is spawned, so a failure
# cannot flood the console. Canary requests count toward no tally.
preflight() {
  local code
  code="$(http_post "$(data_body 0 0 "$(date +%s)")" "$CANARY_BODY_FILE")"
  if [[ "$code" == "000" ]]; then
    echo "✗ Backend unreachable at ${EVENTS_URL}. Start the stack: ./scripts/start.sh" >&2
    exit 1
  fi
  if [[ ! "$code" =~ ^2 ]]; then
    echo "✗ Canary DATA request failed (HTTP ${code}):" >&2
    cat "$CANARY_BODY_FILE" >&2; echo >&2
    exit 1
  fi

  if [[ "$TYPE" == "image" || "$TYPE" == "both" ]]; then
    code="$(http_post "$IMAGE_BODY" "$CANARY_BODY_FILE")"
    if [[ "$code" == "403" ]]; then
      echo "✗ IMAGE events rejected (403): www.gstatic.com not allowlisted." >&2
      echo "  Restart with the testing profile: ./scripts/start.sh --restart --profile testing" >&2
      exit 1
    fi
    if [[ ! "$code" =~ ^2 ]]; then
      echo "✗ Canary IMAGE request failed (HTTP ${code}):" >&2
      cat "$CANARY_BODY_FILE" >&2; echo >&2
      exit 1
    fi
  fi
  echo "✓ preflight ok"
}

# One worker process: loops ITERATIONS times issuing sequential POSTs per the
# TYPE rule (data => 1 DATA, image => 1 IMAGE, both => 1 DATA + 1 IMAGE per
# iteration). Tallies "<ok> <fail>" to RUN_DIR/w<id>. Flood guard: prints at
# most the FIRST non-2xx it sees to stderr, then only counts the rest.
worker() {
  local wid="$1"
  local ok=0 fail=0 sampled=0 i code
  for ((i = 1; i <= ITERATIONS; i++)); do
    if [[ "$TYPE" == "data" || "$TYPE" == "both" ]]; then
      code="$(http_post "$(data_body "$wid" "$i" "$(date +%s)")" /dev/null)"
      if [[ "$code" =~ ^2 ]]; then
        ok=$((ok + 1))
      else
        fail=$((fail + 1))
        if [[ $sampled -eq 0 ]]; then
          echo "[w${wid}] non-2xx DATA (HTTP ${code}) — further errors counted silently" >&2
          sampled=1
        fi
      fi
    fi
    if [[ "$TYPE" == "image" || "$TYPE" == "both" ]]; then
      code="$(http_post "$IMAGE_BODY" /dev/null)"
      if [[ "$code" =~ ^2 ]]; then
        ok=$((ok + 1))
      else
        fail=$((fail + 1))
        if [[ $sampled -eq 0 ]]; then
          echo "[w${wid}] non-2xx IMAGE (HTTP ${code}) — further errors counted silently" >&2
          sampled=1
        fi
      fi
    fi
  done
  echo "${ok} ${fail}" > "${RUN_DIR}/w${wid}"
}

# Spawns THREADS worker processes via xargs -P and waits for them. Records
# wall-clock start/end around the load phase only (preflight excluded).
run_load() {
  export RUN_DIR ITERATIONS TYPE EVENTS_URL MAX_TIME IMAGE_BODY
  export -f worker http_post data_body
  RUN_START=$(date +%s)
  seq 1 "$THREADS" | xargs -P "$THREADS" -I{} bash -c 'worker "$@"' _ {} || true
  RUN_END=$(date +%s)
}

# Sums the per-worker tallies and prints the HTTP-level summary. Exit stays 0
# even with non-2xx (this is a load tool, not a test); the count is surfaced
# loudly in the summary instead.
print_summary() {
  local total_ok=0 total_fail=0 o fl f
  for f in "$RUN_DIR"/w*; do
    [[ -e "$f" ]] || continue
    read -r o fl < "$f"
    total_ok=$((total_ok + o))
    total_fail=$((total_fail + fl))
  done
  TOTAL_OK=$total_ok
  local total=$((total_ok + total_fail))
  local elapsed=$((RUN_END - RUN_START))
  local rate="n/a"
  [[ $elapsed -gt 0 ]] && rate="$((total / elapsed))/s"
  echo "─────────────────────────────────────────────"
  echo "  sent:        ${total}"
  echo "  2xx:         ${total_ok}"
  echo "  non-2xx:     ${total_fail}"
  echo "  wall-clock:  ${elapsed}s"
  echo "  approx rate: ${rate}"
  echo "─────────────────────────────────────────────"
}

# Polls processed_events until its row count rises by the expected number of
# events (successful worker requests + canaries) or VERIFY_TIMEOUT_SECS
# elapses, then prints a MATCH/OVER/MISMATCH/SKIPPED line. The Flink
# event_type_counts_99m and image_size_buckets_99m tables are tumbling windows
# that have not fired yet at this point, so they are intentionally NOT verified
# here (FFU).
verify_counts() {
  local canary expected cur delta="" deadline
  canary="$(canary_count)"
  expected=$(( TOTAL_OK + canary ))
  deadline=$(( $(date +%s) + VERIFY_TIMEOUT_SECS ))
  while true; do
    cur="$(db_processed_count)"
    if [[ "$cur" =~ ^[0-9]+$ && "$BASELINE_COUNT" =~ ^[0-9]+$ ]]; then
      delta=$(( cur - BASELINE_COUNT ))
      if [[ $delta -ge $expected ]]; then break; fi
    fi
    if [[ $(date +%s) -ge $deadline ]]; then break; fi
    sleep 3
  done
  echo "─────────────────────────────────────────────"
  if ! [[ "$cur" =~ ^[0-9]+$ && "$BASELINE_COUNT" =~ ^[0-9]+$ ]]; then
    echo "  verify:  SKIPPED — could not query processed_events (Postgres/Docker up?)"
  elif [[ $delta -eq $expected ]]; then
    echo "  verify:  MATCH — processed_events grew by ${delta} (expected ${expected})"
  elif [[ $delta -gt $expected ]]; then
    echo "  verify:  OVER (${delta} > expected ${expected}) — concurrent traffic during the run?"
  else
    echo "  verify:  MISMATCH (${delta} < expected ${expected}) after ${VERIFY_TIMEOUT_SECS}s — check events-dlq"
  fi
  echo "  note:    event_type_counts_99m, image_size_buckets_99m not verified — tumbling windows; FFU"
  echo "─────────────────────────────────────────────"
}

print_banner
if [[ "$RESET_AND_VERIFY" == true ]]; then
  reset_tables
  BASELINE_COUNT="$(db_processed_count)"
fi
preflight
run_load
print_summary
if [[ "$RESET_AND_VERIFY" == true ]]; then verify_counts; fi
exit 0
