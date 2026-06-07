#!/usr/bin/env bash
# End-to-end check that Flink-health alerting fires on a real outage.
# Stops the Flink TaskManager, polls Grafana's rule-state API until the
# 'Flink target down' rule reaches 'firing', then restarts the TaskManager
# and waits for it to clear. Requires the stack to be running (start.sh).
#
# Exercises the chain end-to-end: Flink reporter -> Prometheus scrape ->
# Grafana rule evaluation. With live SMTP creds in .env, firing also
# dispatches the alert email; delivery itself is not asserted here.
#
# Grafana admin creds default to the container's GF_SECURITY_ADMIN_* (admin/admin).
# If the password was changed in the UI, pass it with -p/--password (and -u/--user),
# or via the GRAFANA_PASS / GRAFANA_USER environment variables.
#
# Usage: alert-test.sh [-u USER] [-p PASSWORD] [-h]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

GF_USER_OPT=""
GF_PASS_OPT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)     sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -u|--user)     GF_USER_OPT="${2:-}"; shift 2 ;;
    -p|--password) GF_PASS_OPT="${2:-}"; shift 2 ;;
    *) echo "Unknown option: $1 (try -h)" >&2; exit 2 ;;
  esac
done

require_docker

TM_SERVICE="flink-taskmanager"
GF_SERVICE="grafana"
TARGET_RULE="Flink target down"
RULES_API="api/prometheus/grafana/api/v1/rules"

# 'for: 1m' + 1m rule-eval interval + 15s scrape: worst-case ~2m to firing, ~1m15s to clear.
FIRE_TIMEOUT=210
CLEAR_TIMEOUT=150

# Credential precedence: -u/-p flags, then GRAFANA_USER/GRAFANA_PASS env, then the container's
# GF_SECURITY_ADMIN_*. The container value only seeds the password at first volume init, so a
# UI-changed password must come from a flag or env var instead.
gf_env() { compose exec -T "$GF_SERVICE" printenv "$1" 2>/dev/null | tr -d '\r'; }
GF_USER="${GF_USER_OPT:-${GRAFANA_USER:-$(gf_env GF_SECURITY_ADMIN_USER)}}"; GF_USER="${GF_USER:-admin}"
GF_PASS="${GF_PASS_OPT:-${GRAFANA_PASS:-$(gf_env GF_SECURITY_ADMIN_PASSWORD)}}"; GF_PASS="${GF_PASS:-admin}"
GF_PORT="$(compose port "$GF_SERVICE" 3000 2>/dev/null | head -1 | sed 's/.*://')"
[[ -n "$GF_PORT" ]] || { echo "✗ $GF_SERVICE is not running (start.sh first)." >&2; exit 1; }
GRAFANA_API="http://localhost:$GF_PORT"

rule_state() {
  curl -s -u "$GF_USER:$GF_PASS" "$GRAFANA_API/$RULES_API" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print(next((r['state'] for g in d.get('data',{}).get('groups',[]) for r in g.get('rules',[]) if r.get('name')==sys.argv[1]),'absent'))" "$TARGET_RULE" 2>/dev/null
}

wait_for_state() {
  local want="$1" timeout="$2" waited=0 state
  while (( waited < timeout )); do
    state="$(rule_state)"
    if [[ "$state" == "$want" ]]; then return 0; fi
    sleep 5; waited=$((waited + 5))
    printf '  … %3ss: "%s" is %s (waiting for %s)\n' "$waited" "$TARGET_RULE" "$state" "$want"
  done
  return 1
}

# Restore only fires if this run actually stopped the TaskManager, so an early exit during
# the preflight (auth failure, missing rule) never churns a TM it never touched.
tm_stopped=0
tm_restored=0
restore_taskmanager() {
  [[ "$tm_stopped" == 1 && "$tm_restored" == 0 ]] || return 0
  tm_restored=1
  echo "↻ Restarting $TM_SERVICE …"
  compose start "$TM_SERVICE" >/dev/null
}
trap restore_taskmanager EXIT

echo "Grafana: $GRAFANA_API"

# Authenticated reachability preflight, before touching the TaskManager: a 401 here would
# otherwise masquerade as a missing rule. Distinguishes auth/connectivity from a real absence.
code="$(curl -s -o /dev/null -w '%{http_code}' -u "$GF_USER:$GF_PASS" "$GRAFANA_API/$RULES_API")"
case "$code" in
  200) ;;
  401) echo "✗ Grafana auth failed (HTTP 401). admin/admin no longer works — the password was likely changed in the UI. Pass it with -p/--password (or GRAFANA_PASS)." >&2; exit 1 ;;
  000) echo "✗ Grafana unreachable at $GRAFANA_API (is the stack running?)." >&2; exit 1 ;;
  *)   echo "✗ Grafana returned HTTP $code for the alert-rules API." >&2; exit 1 ;;
esac

initial="$(rule_state)"
if [[ "$initial" == "absent" ]]; then
  echo "✗ Rule \"$TARGET_RULE\" is not provisioned in Grafana." >&2
  exit 1
fi
if [[ "$initial" != "inactive" ]]; then
  echo "✗ Expected \"$TARGET_RULE\" to start 'inactive' but it is '$initial' — stack not healthy." >&2
  exit 1
fi
echo "✓ Baseline: \"$TARGET_RULE\" is inactive"

echo "⏻ Stopping $TM_SERVICE to induce a target-down outage …"
tm_stopped=1
compose stop "$TM_SERVICE" >/dev/null

if wait_for_state firing "$FIRE_TIMEOUT"; then
  echo "✓ \"$TARGET_RULE\" reached 'firing' after the outage"
else
  echo "✗ \"$TARGET_RULE\" did not fire within ${FIRE_TIMEOUT}s" >&2
  exit 1
fi

restore_taskmanager
if wait_for_state inactive "$CLEAR_TIMEOUT"; then
  echo "✓ \"$TARGET_RULE\" cleared back to 'inactive' after recovery"
else
  echo "✗ \"$TARGET_RULE\" did not clear within ${CLEAR_TIMEOUT}s of recovery" >&2
  exit 1
fi

echo "PASS: alerting fires on outage and clears on recovery"
