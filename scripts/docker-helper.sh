#!/usr/bin/env bash
# Docker Compose helper for the local stack. One entry point for the thin compose ops.
# Usage: docker-helper.sh <command> [args]
#   --build
#       Build all stack images.
#   --stop [--keep-volumes] [--prune-dangling-images]
#       Stop the stack. Default removes volumes; --keep-volumes preserves data.
#       --prune-dangling-images also purges dangling images/volumes (ignored with --keep-volumes).
#   --logs [-e|--errors|-w|--warnings] [--grep <pat>] [--since <dur>] [--sort time [--order asc|desc]] [service]
#       Follow logs live (last 200 lines). -e filters to ERROR/EXCEPTION/FATAL; -w widens that to
#       also include WARN; --grep <pat> filters to a case-insensitive regex; --since limits to
#       recent logs (e.g. 10m, 1h, or an absolute time); --sort time takes a finite snapshot
#       ordered chronologically and exits (--order desc=newest first, default; asc=oldest first);
#       service narrows to one service.
#   -h, --help
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || -z "${1:-}" ]]; then
  sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
  [[ -z "${1:-}" ]] && exit 1
  exit 0
fi

# Builds all images. The frontend build context excludes ../backend, so the payload
# schema is staged into frontend/public where the frontend image can reach it.
do_build() {
  mkdir -p "$ROOT_DIR/frontend/public"
  cp "$ROOT_DIR/backend/src/main/resources/event-payload-schema.json" \
     "$ROOT_DIR/frontend/public/event-payload-schema.json"
  compose build
}

do_stop() {
  if [[ "${1:-}" == "--keep-volumes" ]]; then
    compose down
  else
    compose down -v --remove-orphans
    if [[ "${1:-}" == "--prune-dangling-images" ]]; then
      docker image prune -f
      docker volume prune -f
    fi
  fi
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

require_docker

mode="$1"
shift
case "$mode" in
  --build) do_build "$@" ;;
  --stop)  do_stop "$@" ;;
  --logs)  do_logs "$@" ;;
  *) echo "Unknown command: $mode (expected --build | --stop | --logs; see -h)" >&2; exit 1 ;;
esac
