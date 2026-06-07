#!/usr/bin/env bash
# Send one DATA and/or IMAGE event to the running backend, then show where each
# landed: the processed_events row written by Flink, plus the MinIO object for IMAGE.
# A manual single-shot tool — no threads, iterations, or tallies.
# Real load testing belongs in a dedicated tool (e.g. JMeter), not here.
# Usage: send-event.sh [options]
#   -t, --type TYPE     data | image | both   (default both)
#       --verify-minio  also list the MinIO object for IMAGE events (default off)
#   -h, --help
# Examples:
#   ./scripts/send-event.sh                              # one DATA + one IMAGE event
#   ./scripts/send-event.sh --type image --verify-minio  # IMAGE + confirm object in MinIO
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

TYPE=both
VERIFY_MINIO=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--type)      TYPE="${2:-}"; shift 2 ;;
    --verify-minio) VERIFY_MINIO=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

TYPE="${TYPE,,}"  # accept any case; all downstream comparisons use the lowercase form
case "$TYPE" in
  data|image|both) ;;
  *) echo "type must be one of: data image both (got: '$TYPE')" >&2; exit 1 ;;
esac

load_env
: "${BACKEND_PORT:?BACKEND_PORT not set — is .env present? See .env.example}"
EVENTS_URL="http://localhost:${BACKEND_PORT}/api/events"
IMAGE_BODY='{"eventType":"IMAGE","imageUrl":"https://www.gstatic.com/webp/gallery/1.jpg"}'
MAX_TIME=15
LAND_TIMEOUT_SECS=30

BODY_FILE="$(mktemp)"
trap 'rm -f "$BODY_FILE"' EXIT

# Builds a unique DATA body conforming to event-payload-schema.json (orderNumber +
# quantity + sku, no extra properties). Epoch seconds and $RANDOM keep each send distinct.
data_body() {
  printf '{"eventType":"DATA","payload":{"orderNumber":"ORD-%s-%s","quantity":%d,"sku":"SKU-%s"}}' \
    "$(date +%s)" "$RANDOM" "$(( RANDOM % 1000 + 1 ))" "$RANDOM"
}

# POSTs $1 as JSON to EVENTS_URL, response body to BODY_FILE. Echoes the HTTP
# status, or 000 on a transport failure (connection refused, timeout).
http_post() {
  local code
  code="$(curl -s -o "$BODY_FILE" -w '%{http_code}' --max-time "$MAX_TIME" \
          -H 'Content-Type: application/json' -X POST -d "$1" "$EVENTS_URL" 2>/dev/null)" \
    || code="000"
  printf '%s' "$code"
}

# Polls processed_events for the given id until present or LAND_TIMEOUT_SECS
# elapses. Returns 0 once the row exists, 1 on timeout or if Postgres is
# unreachable, so the caller can report the right outcome without aborting.
wait_for_landing() {
  local id="$1" cur deadline
  deadline=$(( $(date +%s) + LAND_TIMEOUT_SECS ))
  while true; do
    cur="$("$SCRIPT_DIR/sql-helper.sh" -tA -c \
      "SELECT count(*) FROM processed_events WHERE id = '${id}';" 2>/dev/null \
      | tr -d '[:space:]')" || cur=""
    [[ "$cur" == "1" ]] && return 0
    [[ $(date +%s) -ge $deadline ]] && return 1
    sleep 2
  done
}

# Sends one event of the given type and reports where it landed. Returns non-zero
# on any failure (send rejected, transport error, or no landing) so the caller can
# aggregate an overall exit status across multiple types.
send_one() {
  local type="$1" body code id object_key
  [[ "$type" == "data" ]] && body="$(data_body)" || body="$IMAGE_BODY"

  echo "→ POST ${type} event to ${EVENTS_URL}"
  code="$(http_post "$body")"

  if [[ "$code" == "000" ]]; then
    echo "✗ Backend unreachable at ${EVENTS_URL}. Start the stack: ./scripts/start.sh" >&2
    return 1
  fi
  if [[ "$code" == "403" && "$type" == "image" ]]; then
    echo "✗ IMAGE event rejected (403): the imageUrl host is not in IMAGE_URL_ALLOWED_HOSTS." >&2
    echo "  Allowlist www.gstatic.com in .env (or use the testing profile) and retry." >&2
    return 1
  fi
  if [[ ! "$code" =~ ^2 ]]; then
    echo "✗ Send failed (HTTP ${code}):" >&2
    cat "$BODY_FILE" >&2; echo >&2
    return 1
  fi

  id="$(grep -o '"id":"[^"]*"' "$BODY_FILE" | head -1 | sed 's/.*:"//;s/"$//')"
  echo "✓ accepted (HTTP ${code}) id=${id}"
  if [[ -z "$id" ]]; then
    echo "  could not parse event id from response; skipping landing check:" >&2
    cat "$BODY_FILE" >&2; echo >&2
    return 1
  fi

  echo "→ waiting up to ${LAND_TIMEOUT_SECS}s for Flink to land it in processed_events…"
  if ! wait_for_landing "$id"; then
    echo "✗ not in processed_events after ${LAND_TIMEOUT_SECS}s — check Flink (./scripts/docker-helper.sh --logs) and events-dlq." >&2
    echo "  (Postgres must be up for this check.)" >&2
    return 1
  fi

  echo "✓ landed:"
  "$SCRIPT_DIR/sql-helper.sh" -c \
    "SELECT id, event_type, source, payload, image_object_key, inserted_at
       FROM processed_events WHERE id = '${id}';"

  # The landed row's image_object_key is set by Flink only after the MinIO upload succeeds,
  # so the object's presence is already implied. --verify-minio adds an explicit listing
  # (which spins up a one-off mc container) for when that extra confirmation is wanted.
  if [[ "$type" == "image" && "$VERIFY_MINIO" == true ]]; then
    object_key="$("$SCRIPT_DIR/sql-helper.sh" -tA -c \
      "SELECT image_object_key FROM processed_events WHERE id = '${id}';" 2>/dev/null \
      | tr -d '[:space:]')" || object_key=""
    if [[ -n "$object_key" ]]; then
      echo "→ MinIO object:"
      "$SCRIPT_DIR/minio-helper.sh" ls "$object_key" || echo "  (could not list ${object_key})" >&2
    fi
  fi
}

rc=0
if [[ "$TYPE" == "data" || "$TYPE" == "both" ]]; then
  send_one data || rc=1
fi
if [[ "$TYPE" == "both" ]]; then echo; fi
if [[ "$TYPE" == "image" || "$TYPE" == "both" ]]; then
  send_one image || rc=1
fi
exit "$rc"
