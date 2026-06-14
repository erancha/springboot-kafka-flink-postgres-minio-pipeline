#!/usr/bin/env bash
# Docker Compose helper for the local stack. One entry point for the thin compose ops.
# Usage: docker-helper.sh <command> [args]
#   --up [--build] [--recreate] [service...]
#       Start the stack, or only the named services. --build rebuilds images first; --recreate
#       forces container recreation. Rebuilding flink-job also recycles the JobManager/TaskManager
#       so the prior job submission is cleared before the new one is resubmitted.
#   --build [service...]
#       Build all stack images, or only the named services.
#   --stop [--keep-volumes] [--prune[=images|volumes|both]]
#       Stop the whole stack. Default removes this stack's volumes; --keep-volumes preserves them.
#       --prune additionally clears this project's dangling images and/or volumes (bare --prune =
#       both), label-scoped to this compose project so other stacks are never touched. --prune with
#       volumes/both is rejected together with --keep-volumes.
#   --logs [-e|--errors|-w|--warnings] [--grep <pat>] [--since <dur>] [--sort time [--order asc|desc]] [service...]
#       Follow logs live (last 200 lines). -e filters to ERROR/EXCEPTION/FATAL; -w widens that to
#       also include WARN; --grep <pat> filters to a case-insensitive regex; --since limits to
#       recent logs (e.g. 10m, 1h, or an absolute time); --sort time takes a finite snapshot
#       ordered chronologically and exits (--order desc=newest first, default; asc=oldest first);
#       a trailing service list narrows to those services.
#   --kafka-disk
#       Show on-disk size of Kafka's log dir: total plus a per-topic breakdown (partitions summed).
#   -h, --help
#
# Scope to specific services by naming them last (narrows --build and --logs; --stop is whole-stack):
#   ./scripts/docker-helper.sh --build backend ui      # include: build just these two
#   ./scripts/docker-helper.sh --logs -e flink-job     # include: follow errors from one service
# To skip a service, list the others. Combine with -e/-w to filter by severity too — 
# e.g. all warnings except Grafana, whose auth logs otherwise dominate -w:
#   ./scripts/docker-helper.sh --logs -w $(docker compose --project-directory . -f scripts/docker-compose.yml config --services 2>/dev/null | grep -vx grafana)
#   ./scripts/docker-helper.sh --logs -w --since 1h $(docker compose --project-directory . -f scripts/docker-compose.yml config --services 2>/dev/null | grep -vx grafana)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || -z "${1:-}" ]]; then
  sed -n '2,31p' "$0" | sed 's/^# \{0,1\}//'
  [[ -z "${1:-}" ]] && exit 1
  exit 0
fi

# Builds the stack images, or only the services named in "$@".
do_build() {
  stage_payload_schema
  compose build "$@"
}

# Compose's default project name: the sanitized basename of the project dir (lowercase, only
# a-z0-9_-). Used to label-scope prune so it only ever touches THIS stack's images/volumes.
compose_project() {
  basename "$ROOT_DIR" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9_-'
}

# Starts the stack, or only the named services. Rebuilding flink-job recycles the
# JobManager/TaskManager: the JobManager keeps the prior submission running across a plain recreate,
# so the cluster pair must be recreated to clear the old job before the rebuilt one is resubmitted.
do_up() {
  local build=false recreate=false
  local -a targets=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --build)    build=true ;;
      --recreate) recreate=true ;;
      -*)         echo "Unknown --up option '$1'" >&2; exit 1 ;;
      *)          targets+=("$1") ;;
    esac
    shift
  done

  if printf '%s\n' "${targets[@]+"${targets[@]}"}" | grep -qx flink-job; then
    targets=(flink-jobmanager flink-taskmanager "${targets[@]}")
    recreate=true
  fi

  # Plain `up -d` still builds images absent on first run, so stage the schema either way.
  stage_payload_schema

  # Materialize the backend scrape target before Prometheus mounts it.
  load_env
  render_prometheus_backend_target "${BACKEND_METRICS_TARGET:-}"

  local -a args=(up -d)
  if $build; then args+=(--build); fi
  if $recreate; then args+=(--force-recreate); fi
  compose "${args[@]}" "${targets[@]+"${targets[@]}"}"
  compose ps
}

# Stops the whole stack. Default removes this stack's volumes (down -v); --keep-volumes preserves
# them. --prune additionally clears this project's dangling images and/or volumes, label-scoped to
# the compose project so other running stacks are never touched.
do_stop() {
  local keep=false prune=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --keep-volumes) keep=true ;;
      --prune)        prune=both ;;
      --prune=*)      prune="${1#*=}" ;;
      *)              echo "Unknown --stop option '$1'" >&2; exit 1 ;;
    esac
    shift
  done

  case "$prune" in
    ""|images|volumes|both) ;;
    *) echo "--prune expects images|volumes|both (bare --prune = both)" >&2; exit 1 ;;
  esac
  if $keep && [[ "$prune" == volumes || "$prune" == both ]]; then
    echo "--keep-volumes cannot combine with --prune=volumes|both — it would delete the kept volumes." >&2
    exit 1
  fi

  if $keep; then
    compose down
  else
    compose down -v --remove-orphans
  fi

  local -a scope=(--filter "label=com.docker.compose.project=$(compose_project)")
  case "$prune" in
    images)  docker image  prune -f "${scope[@]}" ;;
    volumes) docker volume prune -f "${scope[@]}" ;;
    both)    docker image  prune -f "${scope[@]}"; docker volume prune -f "${scope[@]}" ;;
  esac
}

# Filtering and follow-vs-snapshot are independent: the severity presets (-e errors, -w warnings
# and worse) and --grep (custom regex) only choose what to match, and the stream follows live by
# default. --sort time is the sole switch to a finite snapshot — it must stop following, since an
# unbounded stream cannot be sorted. --since bounds the window either way. Remaining args after the
# flags name a single service.
do_logs() {
  local pattern="" follow=true sort_by="" order="desc"
  local -a since=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -e|--errors)   pattern='(error|exception|fatal)'; shift ;;
      -w|--warnings) pattern='(warn|warning|error|exception|fatal)'; shift ;;
      --grep)        pattern="${2:?--grep needs a pattern}"; shift 2 ;;
      --since)       since=(--since "${2:?--since needs a duration}"); shift 2 ;;
      --sort)
        [[ "${2:?--sort needs a field (time)}" == time ]] || { echo "--sort: only 'time' is supported" >&2; exit 1; }
        sort_by=time; follow=false; shift 2 ;;
      --order)
        order="${2:?--order needs asc|desc}"
        [[ "$order" == asc || "$order" == desc ]] || { echo "--order: expected asc|desc" >&2; exit 1; }
        shift 2 ;;
      *)             break ;;
    esac
  done

  local -a args=(logs "${since[@]}")
  if "$follow"; then args+=(-f --tail=200); fi
  # -t prepends a uniform docker timestamp so lines from services that each log in their own clock
  # format share one sortable field; only added when sorting, to leave the default output untouched.
  if [[ -n "$sort_by" ]]; then args+=(-t); fi
  args+=("$@")

  if [[ "$sort_by" == time ]]; then
    # Sort on the -t stamp (field 2, after the "<service> |" prefix; -r for newest first) to weave
    # the per-service blocks into one stream, then collapse the stamp to HH:MM:SS — it was only a
    # sort key, and on recent logs the date is redundant and the nanoseconds are noise.
    local -a sort=(sort -t'|' -k2)
    [[ "$order" == desc ]] && sort+=(-r)
    local trim='s/\| [0-9]{4}-[0-9]{2}-[0-9]{2}T([0-9:]{8})\.[0-9]+Z /| \1 | /'
    if [[ -n "$pattern" ]]; then
      compose "${args[@]}" | grep -Eai "$pattern" | "${sort[@]}" | sed -E "$trim"
    else
      compose "${args[@]}" | "${sort[@]}" | sed -E "$trim"
    fi
  else
    if [[ -n "$pattern" ]]; then
      compose "${args[@]}" | grep -Eai "$pattern"
    else
      compose "${args[@]}"
    fi
  fi
}

# Kafka stores partition logs under log.dirs (image default /tmp/kafka-logs unless KAFKA_LOG_DIRS
# overrides it). Both paths are resolved inside the container so the report tracks wherever the
# broker actually writes. Partition dirs are named <topic>-<n>; summing by stripping that suffix
# yields per-topic totals.
do_kafka_disk() {
  compose exec -T kafka sh -c '
    dir="${KAFKA_LOG_DIRS:-/tmp/kafka-logs}"
    [ -d "$dir" ] || { echo "no kafka log dir at $dir" >&2; exit 1; }
    printf "total  %s  %s\n\n" "$(du -sh "$dir" | cut -f1)" "$dir"
    du -sm "$dir"/*/ 2>/dev/null | awk "
      { n=split(\$2, p, \"/\"); base=(p[n]==\"\")?p[n-1]:p[n];
        topic=base; sub(/-[0-9]+\$/, \"\", topic);
        mb[topic]+=\$1; parts[topic]++ }
      END { for (t in mb) printf \"%-22s %6d MB  (%d part%s)\n\",
              t, mb[t], parts[t], (parts[t]==1?\"\":\"s\") }
    " | sort -k2 -rn
  '
}

require_docker

mode="$1"
shift
case "$mode" in
  --up)         do_up "$@" ;;
  --build)      do_build "$@" ;;
  --stop)       do_stop "$@" ;;
  --logs)       do_logs "$@" ;;
  --kafka-disk) do_kafka_disk ;;
  *) echo "Unknown command: $mode (expected --up | --build | --stop | --logs | --kafka-disk; see -h)" >&2; exit 1 ;;
esac
