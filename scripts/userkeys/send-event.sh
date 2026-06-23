#!/usr/bin/env bash
# Send a burst of userKeys events for one (userId, key) pair, then show the windowed sum Flink wrote
# to Postgres. A manual single-shot tool — no threads or tallies; real load testing belongs in
# JMeter (scripts/jmeter-helper.sh --pipeline userkeys).
#
# The pipeline writes only windowed aggregates: a (userId, key) row appears only once its event-time
# tumbling window closes, which needs the watermark to advance past the window end. A lone event
# never closes its own window. So this tool sends the burst, then emits a trickle of heartbeat events
# on a throwaway key (which advance event time without changing the burst key's sum) until the window
# closes and the aggregate row lands.
# Usage: send-event.sh [options]
#       --count N     number of burst events for the demo key (default 5)
#   -h, --help
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
source "$SCRIPT_DIR/../common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

COUNT=5
while [[ $# -gt 0 ]]; do
  case "$1" in
    --count) COUNT="${2:?--count needs a number}"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

load_env
: "${BACKEND_PORT:?BACKEND_PORT not set — is .env present? See .env.example}"
URL="http://localhost:${BACKEND_PORT}/api/user-keys"
WINDOW_SECS="${USERKEYS_WINDOW_SECONDS:-60}"
MAX_TIME=15
# The window closes once event time advances a full window past the burst, plus the out-of-orderness
# bound; heartbeats run a little longer so a checkpoint can commit the result before the poll.
HEARTBEAT_SECS=$(( WINDOW_SECS + 25 ))

# A user id unique to this run, so the verification sum reflects only this run's events.
DEMO_USER="demo-$(date +%s)-$RANDOM"
DEMO_KEY="A"
WATERMARK_KEY="__watermark__"

BODY_FILE="$(mktemp)"
trap 'rm -f "$BODY_FILE"' EXIT

# POSTs a {userId, key, value} event; echoes the HTTP status, or 000 on a transport failure.
post_event() {
  local user="$1" key="$2" value="$3" code
  code="$(curl -s -o "$BODY_FILE" -w '%{http_code}' --max-time "$MAX_TIME" \
          -H 'Content-Type: application/json' -X POST \
          -d "{\"userId\":\"${user}\",\"key\":\"${key}\",\"value\":${value}}" "$URL" 2>/dev/null)" \
    || code="000"
  printf '%s' "$code"
}

# Sum of the burst values 1..COUNT scaled by 100 — a known total to verify the aggregate against.
expected=0
echo "→ sending ${COUNT} burst events for userId=${DEMO_USER} key=${DEMO_KEY} to ${URL}"
for i in $(seq 1 "$COUNT"); do
  value=$(( i * 100 ))
  code="$(post_event "$DEMO_USER" "$DEMO_KEY" "$value")"
  if [[ "$code" == "000" ]]; then
    echo "✗ Backend unreachable at ${URL}. Start the stack: ./scripts/start.sh --pipeline userkeys" >&2
    exit 1
  fi
  if [[ ! "$code" =~ ^2 ]]; then
    echo "✗ Send failed (HTTP ${code}):" >&2; cat "$BODY_FILE" >&2; echo >&2; exit 1
  fi
  expected=$(( expected + value ))
done
echo "✓ burst accepted; expected window sum for key ${DEMO_KEY} = ${expected}"

echo "→ advancing the watermark for ~${HEARTBEAT_SECS}s (window=${WINDOW_SECS}s) so the window closes…"
target_sum=""
deadline=$(( $(date +%s) + HEARTBEAT_SECS ))
while [[ $(date +%s) -lt $deadline ]]; do
  post_event "$DEMO_USER" "$WATERMARK_KEY" 0 >/dev/null
  target_sum="$("$ROOT_DIR/scripts/sql-helper.sh" --db userkeys -tA -c \
    "SELECT COALESCE(SUM(value_sum), 0) FROM user_key_aggregates WHERE user_id = '${DEMO_USER}' AND key = '${DEMO_KEY}';" \
    2>/dev/null | tr -d '[:space:]')" || target_sum=""
  [[ "$target_sum" =~ ^[0-9]+$ && "$target_sum" -ge "$expected" ]] && break
  sleep 2
done

if [[ ! "$target_sum" =~ ^[0-9]+$ || "$target_sum" -lt "$expected" ]]; then
  echo "✗ aggregate sum for key ${DEMO_KEY} reached '${target_sum:-<none>}', expected ${expected} —" >&2
  echo "  check Flink (./scripts/docker-helper.sh --logs) and ${KAFKA_USERKEYS_DLQ_TOPIC:-user-keys-dlq}." >&2
  exit 1
fi

echo "✓ landed (sum=${target_sum}):"
"$ROOT_DIR/scripts/sql-helper.sh" --db userkeys -c \
  "SELECT window_start, window_end, user_id, key, value_sum
     FROM user_key_aggregates WHERE user_id = '${DEMO_USER}' ORDER BY window_start, key;"
