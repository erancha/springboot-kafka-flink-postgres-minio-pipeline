#!/usr/bin/env bash
# Collect one textual evidence bundle for a pipeline load-test investigation.
# Usage: diagnose.sh [--since 3h] [--step 30]
# Sections: stack status, run timeline + per-run metrics, alerts fired/resolved,
# Postgres table status, recent error signatures. Interpretation is the caller's job (see SKILL.md).
# The Docker stack must be up; this reads from it, it does not start or change it.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SINCE="3h"
STEP="30"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --since) SINCE="$2"; shift 2 ;;
    --step) STEP="$2"; shift 2 ;;
    -h|--help) sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

# Walk up to the repo root (the dir holding scripts/docker-compose.yml) so this skill
# works regardless of the cwd the caller invokes it from.
REPO_DIR="$SCRIPT_DIR"
while [[ "$REPO_DIR" != "/" && ! -f "$REPO_DIR/scripts/docker-compose.yml" ]]; do
  REPO_DIR="$(dirname "$REPO_DIR")"
done
if [[ ! -f "$REPO_DIR/scripts/docker-compose.yml" ]]; then
  echo "Could not locate the repo root (scripts/docker-compose.yml)." >&2
  exit 1
fi

# docker logs accepts the raw duration (3h/90m); Prometheus needs seconds.
case "$SINCE" in
  *h) SINCE_SECS=$(( ${SINCE%h} * 3600 )) ;;
  *m) SINCE_SECS=$(( ${SINCE%m} * 60 )) ;;
  *s) SINCE_SECS=${SINCE%s} ;;
  *)  SINCE_SECS=$SINCE ;;
esac

# Pull PROMETHEUS_PORT (and friends) from the repo .env without disturbing this shell's set -u.
PROM_PORT=9090
if [[ -f "$REPO_DIR/.env" ]]; then
  PROM_PORT="$(grep -E '^PROMETHEUS_PORT=' "$REPO_DIR/.env" | tail -1 | cut -d= -f2 || true)"
  PROM_PORT="${PROM_PORT:-9090}"
fi
PROM_URL="http://localhost:${PROM_PORT}"

compose() { (cd "$REPO_DIR" && docker compose --project-directory "$REPO_DIR" -f "$REPO_DIR/scripts/docker-compose.yml" "$@"); }
rule() { printf '\n========== %s ==========\n' "$1"; }

rule "STACK STATUS"
compose ps --format '{{.Service}}\t{{.Status}}' 2>/dev/null || {
  echo "Stack not reachable. Start it with ./scripts/start.sh" >&2; exit 2; }

rule "RUN TIMELINE & PER-RUN METRICS  (window=${SINCE})"
python3 "$SCRIPT_DIR/prom_snapshot.py" --prom-url "$PROM_URL" --since "$SINCE_SECS" --step "$STEP"

rule "ALERTS FIRED / RESOLVED  (since ${SINCE})"
# Grafana records alert state transitions in its SQLite store. Copy that db out of the container
# (read-only) and pair each fired->resolved episode, so alert windows sit next to the run windows
# above them. Reading the file needs no Grafana credentials.
alerts() {
  local cid db
  cid="$(compose ps -q grafana 2>/dev/null || true)"
  if [[ -z "$cid" ]]; then echo "(grafana not running)"; return; fi
  db="$(mktemp "${TMPDIR:-/tmp}/grafana_db.XXXXXX")"
  if docker cp "$cid:/var/lib/grafana/grafana.db" "$db" >/dev/null 2>&1; then
    python3 "$SCRIPT_DIR/grafana_alerts.py" --db "$db" --since "$SINCE_SECS"
  else
    echo "(could not read grafana.db)"
  fi
  rm -f "$db"
}
alerts

rule "POSTGRES TABLE STATUS"
# n_dead_tup, last_autovacuum and total size per table reveal bloat and the table-growth
# that makes a later run slower than an identical earlier one.
"$REPO_DIR/scripts/sql-helper.sh" -c "
SELECT relname,
       n_live_tup, n_dead_tup, n_tup_ins,
       pg_size_pretty(pg_total_relation_size(relid))  AS total,
       pg_size_pretty(pg_indexes_size(relid))         AS indexes,
       last_autovacuum, autovacuum_count
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;" 2>/dev/null || echo "(postgres query failed)"

"$REPO_DIR/scripts/sql-helper.sh" -c "
SELECT current_setting('max_connections')        AS max_connections,
       current_setting('shared_buffers')         AS shared_buffers,
       (SELECT count(*) FROM pg_stat_activity)    AS current_connections;" 2>/dev/null || true

rule "RECENT ERROR SIGNATURES  (since ${SINCE})"
scan() { # $1=service  $2=egrep pattern
  local out
  out="$(compose logs --since "$SINCE" --no-log-prefix "$1" 2>/dev/null | grep -iE "$2" | tail -15 || true)"
  printf -- '--- %s ---\n%s\n' "$1" "${out:-(none)}"
}
scan postgres            'fatal|cancel|deadlock|could not|timeout|terminat'
scan kafka               'overloaded|request_timed_out|notcoordinator|coordinatorload|error'
scan flink-taskmanager   'switched from running to failed|exception|restored|checkpoint .*expired|thread starvation|clock leap'
scan flink-jobmanager    'restart|recover|checkpoint .*expired|exception|failed to (trigger|complete)'
scan backend             'error|exception|fail'
