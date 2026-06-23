#!/usr/bin/env bash
# Load-test a pipeline's backend ingestion path with Apache JMeter.
# Drives that pipeline's jmeter/<pipeline>/backend-load.jmx plan (default pipeline: eventtype), which
# owns the endpoint it floods; the helper supplies only the backend base host:port. Asserts a 2xx per
# request. Use the pipeline's send-event.sh for single-shot, end-to-end checks; this tool is purely
# about ingestion throughput.
# Usage: jmeter-helper.sh [options]
#   -t, --threads N      concurrent virtual users   (default 10)
#   -i, --iterations N   requests per thread         (default 10)
#       --backend-host H target a backend on another host         (default localhost)
#       --pipeline NAME  pipeline whose plan and preflight sender to use (default eventtype)
#       --advise N       sample docker stats over an N-minute window covering the run,
#                        then recommend memory/parallelism knob changes from the peaks
#       --skip-preflight skip the send-event.sh pipeline check; flood the backend
#                        even when Flink/MinIO downstream is unhealthy (edge case)
#   -h, --help
# Total requests sent = threads x iterations.
# Examples:
#   ./scripts/jmeter-helper.sh                     # 10 threads x 10 = 100 requests
#   ./scripts/jmeter-helper.sh -t 50 -i 20         # 50 threads x 20 = 1000 requests
#   ./scripts/jmeter-helper.sh -t 50 -i 50 --advise 5   # load inside a 5 min window, advise tuning
#   ./scripts/jmeter-helper.sh --backend-host 10.0.0.20 -t 500 -i 20000  # flood a backend on another host
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

JMETER_VERSION=5.6.3
JMETER_HOME_DIR="$ROOT_DIR/jmeter"
RUNTIME_DIR="$JMETER_HOME_DIR/.runtime"
RESULTS_DIR="$JMETER_HOME_DIR/results"
DOWNLOAD_BASE="https://archive.apache.org/dist/jmeter/binaries"

THREADS=10
ITERATIONS=10
BACKEND_HOST=localhost
PIPELINE=eventtype
SKIP_PREFLIGHT=false
ADVISE_MINUTES=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--threads)     THREADS="${2:-}"; shift 2 ;;
    -i|--iterations)  ITERATIONS="${2:-}"; shift 2 ;;
    --backend-host)   BACKEND_HOST="${2:-}"; shift 2 ;;
    --pipeline)       PIPELINE="${2:-}"; shift 2 ;;
    --advise)         ADVISE_MINUTES="${2:-}"; shift 2 ;;
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

[[ -n "$BACKEND_HOST" ]] || { echo "--backend-host requires a non-empty value" >&2; exit 1; }
[[ -n "$PIPELINE" ]] || { echo "--pipeline requires a non-empty value" >&2; exit 1; }

# Per-pipeline artifacts: the JMeter plan flooded at the backend, and the single-shot sender used as
# the downstream preflight. A new pipeline drops both under jmeter/<pipeline>/ and scripts/<pipeline>/.
TEST_PLAN="$JMETER_HOME_DIR/$PIPELINE/backend-load.jmx"
PREFLIGHT_SENDER="$SCRIPT_DIR/$PIPELINE/send-event.sh"

# A 0-minute window advises on the load run alone (no observation beyond it).
if [[ -n "$ADVISE_MINUTES" && ! "$ADVISE_MINUTES" =~ ^[0-9]+$ ]]; then
  echo "--advise minutes must be a non-negative integer (got: '$ADVISE_MINUTES')" >&2
  exit 1
fi

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

ADVISE_SAMPLE_SECONDS=5

# Background sampler: append one docker-stats line per container every few seconds for the
# whole load + drain window. Peaks (not the idle endpoints) are what reveal saturation, so the
# samples are the basis for tuning advice rather than a single before/after snapshot.
start_stats_sampler() {
  local out="$1" ids="$2"
  # The subshell's own stdout/stderr are detached to /dev/null: were they left on the caller's
  # command-substitution pipe, "$(start_stats_sampler ...)" would block until this never-ending
  # loop closed the pipe — i.e. forever. Samples go to "$out" via the explicit redirect.
  ( while :; do
      docker stats --no-stream \
        --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}' $ids >> "$out" 2>/dev/null || true
      sleep "$ADVISE_SAMPLE_SECONDS"
    done ) >/dev/null 2>&1 &
  echo $!
}

# Reads the three coupled parallelism knobs, preferring the running stack over the declared value so
# advice reflects what is actually in effect. PIPELINE_PARALLELISM (.env, loaded by load_env) is the
# single source of truth for all three; the live reads can still diverge from it (e.g. a TaskManager
# not yet up, or partitions altered out of band). Echoes one line:
#   "<slots> <partitions> <jobParallelism> <slotsSrc> <partsSrc>"   (srcs: live | env)
# slots: total Flink execution slots (JobManager REST). partitions: live count for the events topic.
# jobParallelism: the submit "-p" — no REST source, so it equals the declared value by construction.
read_parallelism_knobs() {
  local declared="${PIPELINE_PARALLELISM:-0}"
  local slots parts slots_src=live parts_src=live

  slots="$(compose exec -T flink-jobmanager curl -fsS http://localhost:8081/overview 2>/dev/null \
           | grep -oE '"slots-total":[0-9]+' | grep -oE '[0-9]+' | head -1 || true)"
  [[ -n "$slots" ]] || { slots_src=env; slots="$declared"; }

  parts="$(compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server "localhost:${KAFKA_PORT}" --describe --topic "${KAFKA_TOPIC}" 2>/dev/null \
           | grep -oE 'PartitionCount:[[:space:]]*[0-9]+' | grep -oE '[0-9]+' | head -1 || true)"
  [[ -n "$parts" ]] || { parts_src=env; parts="$declared"; }

  echo "${slots:-0} ${parts:-0} ${declared} ${slots_src} ${parts_src}"
}

# Turn the captured samples into a per-service peak table plus knob-level advice. The only tunables
# in this stack are each service's deploy.resources.limits.memory (scripts/docker-compose.yml) and the
# coupled parallelism read live by read_parallelism_knobs.
advise_tune() {
  local samples="$1"
  if [[ ! -s "$samples" ]]; then
    echo "No docker stats samples captured; cannot advise (was the stack running?)." >&2
    return
  fi

  local cores; cores="$(nproc 2>/dev/null || echo 1)"
  local name_to_svc; name_to_svc="$(compose ps --format '{{.Name}}\t{{.Service}}' 2>/dev/null || true)"
  local slots parts jobp slots_src parts_src
  read -r slots parts jobp slots_src parts_src <<<"$(read_parallelism_knobs)"

  # Collapse the raw samples to one row per container: average AND peak for CPU and mem%%. Average
  # reflects sustained pressure (the basis for the CPU-bound call); peak catches spikes and bounds
  # OOM risk. The used/limit pair (whatever docker units) is normalized to MiB at the mem peak so
  # advice can reason about absolute reclaimable RAM.
  local peaks
  peaks="$(awk -F'\t' '
    function tomib(x,  n,u){ n=x+0; u=x; gsub(/[0-9. ]/,"",u);
      if (u=="GiB"||u=="GB") return n*1024;
      if (u=="KiB"||u=="kB"||u=="KB") return n/1024;
      if (u=="B") return n/1048576;
      return n }
    { c=$2; sub(/%/,"",c); m=$4; sub(/%/,"",m);
      cnt[$1]++; csum[$1]+=c+0; msum[$1]+=m+0;
      if (c+0 > cpu[$1]) cpu[$1]=c+0;
      if (cnt[$1] == 1 || m+0 > mem[$1]) { mem[$1]=m+0; usage[$1]=$3 } }
    END { for (n in cnt) { split(usage[n], a, " / ");
            printf "%s\t%.0f\t%.0f\t%.0f\t%.0f\t%s\t%.0f\t%.0f\n",
              n, csum[n]/cnt[n], cpu[n], msum[n]/cnt[n], mem[n], usage[n], tomib(a[1]), tomib(a[2]) } }
  ' "$samples")"

  echo
  echo "──────── Resource avg/peak (${ADVISE_MINUTES:-0} min observation window, ${cores} host cores) ────────"
  printf '  %-18s %8s %8s %8s %8s   %s\n' "SERVICE" "CPU avg" "CPU pk" "MEM avg" "MEM pk" "USAGE / LIMIT"
  while IFS=$'\t' read -r name avgcpu peakcpu avgmem peakmem usage usedmib limmib; do
    [[ -n "$name" ]] || continue
    local svc; svc="$(awk -F'\t' -v n="$name" '$1==n{print $2; exit}' <<<"$name_to_svc")"
    printf '  %-18s %7s%% %7s%% %7s%% %7s%%   %s\n' "${svc:-$name}" "$avgcpu" "$peakcpu" "$avgmem" "$peakmem" "$usage"
  done <<<"$(sort <<<"$peaks")"

  echo
  echo "──────── Tuning advice (heuristic, from avg/peak above) ────────"
  # Flag a cap for trimming only when it is BOTH genuinely under-used (≤30% peak — a service at ~50%
  # is healthy, not over-provisioned) AND has slack worth reclaiming on the 8GB VM. The MiB gate also
  # spares already-tiny caps, where a low percentage frees almost nothing (30% of 128m is ~38m).
  local over_provisioned_pct=30 min_reclaim_mib=128
  local mem_advice=false tm_avgcpu="" tm_peakcpu="" trim="" flink_job_idle=false
  while IFS=$'\t' read -r name avgcpu peakcpu avgmem peakmem usage usedmib limmib; do
    [[ -n "$name" ]] || continue
    local svc; svc="$(awk -F'\t' -v n="$name" '$1==n{print $2; exit}' <<<"$name_to_svc")"
    svc="${svc:-$name}"
    [[ "$svc" == "flink-taskmanager" ]] && { tm_avgcpu="$avgcpu"; tm_peakcpu="$peakcpu"; }
    local reclaim=$(( limmib - usedmib ))
    if (( peakmem >= 85 )); then
      echo "  • ${svc}: peak mem ${peakmem}% (${usedmib}MiB of ${limmib}MiB) — raise its" \
           "deploy.resources.limits.memory in scripts/docker-compose.yml (OOM-kill risk under this load)."
      mem_advice=true
    elif [[ "$svc" == flink-job-* ]] && (( peakmem <= over_provisioned_pct )); then
      # The job submitter idles on `tail -f /dev/null` after submitting; its real peak is the
      # submit-time client JVM at startup, before sampling begins. The sampled idle is not a basis
      # for trimming. The active pipeline's submitter is flink-job-eventtype or flink-job-userkeys.
      flink_job_idle=true
    elif (( peakmem <= over_provisioned_pct && reclaim >= min_reclaim_mib )); then
      trim+="${reclaim}	${svc}	${peakmem}	${usedmib}	${limmib}"$'\n'
    fi
  done <<<"$peaks"

  # Largest reclaimable first, so the most worthwhile trim leads.
  if [[ -n "$trim" ]]; then
    while IFS=$'\t' read -r reclaim svc peakmem usedmib limmib; do
      [[ -n "$svc" ]] || continue
      echo "  • ${svc}: peak mem ${peakmem}% (${usedmib}MiB of ${limmib}MiB) — ~${reclaim}MiB of its cap" \
           "is unused; lower limits.memory if another service needs the RAM."
      mem_advice=true
    done <<<"$(sort -t$'\t' -k1 -rn <<<"$trim")"
  fi

  $flink_job_idle && echo "  • job submitter: idle here, but its cap is not — keep it. The submit-time client" \
    "JVM peaks at startup (before sampling starts), so the sampled idle understates what it needs."

  # Source parallelism can't exceed the partition count, so TaskManager CPU headroom — not slot
  # count — decides whether raising parallelism could help. Judge on the AVERAGE (sustained load),
  # not a single-sample peak that a GC or checkpoint flush can briefly inflate.
  if [[ -n "$tm_avgcpu" ]]; then
    local tm_ceiling=$(( cores * 100 ))
    if (( tm_avgcpu >= tm_ceiling * 80 / 100 )); then
      echo "  • flink-taskmanager: CPU-bound (avg ${tm_avgcpu}% of ${tm_ceiling}% ≈ ${cores} cores saturated)." \
           "Source parallelism is already at the ${parts}-partition ceiling, so add vCPUs to the" \
           "WSL2 VM (~/.wslconfig) rather than slots."
    else
      echo "  • flink-taskmanager: CPU avg ${tm_avgcpu}% / peak ${tm_peakcpu}% of ${tm_ceiling}% (${cores}" \
           "cores) — not CPU-bound. If throughput lags, the limit is upstream (backend/Kafka), not Flink slots."
    fi
  fi

  if (( slots > 0 && parts > 0 )); then
    local src_cap=$(( parts < slots ? parts : slots ))
    echo "  • Parallelism in effect: -p ${jobp} requested, ${slots} TaskManager slots [${slots_src}]," \
         "${parts} ${KAFKA_TOPIC}-topic partitions [${parts_src}]."
    if (( parts == slots && slots == jobp )); then
      echo "    Balanced at ${src_cap}; source throughput is capped here. To lift it, raise all three" \
           "together — taskmanager.numberOfTaskSlots, the flink-job \"-p\" flag, and the topic partitions."
    else
      echo "    Effective source parallelism = min(partitions ${parts}, slots ${slots}) = ${src_cap};" \
           "the smallest is the bottleneck — raise it first (keep -p ≤ slots); lifting the others alone won't help."
    fi
  fi

  $mem_advice || echo "  • Memory caps all sit in a comfortable band; no limits.memory change indicated."
}

load_env
: "${BACKEND_PORT:?BACKEND_PORT not set — is .env present? See .env.example}"
# Pass only the backend base; each pipeline's plan appends its own endpoint path (e.g. /api/events,
# /api/user-keys), so the helper stays agnostic to which route a pipeline ingests on.
BACKEND_URL="http://${BACKEND_HOST}:${BACKEND_PORT}"

[[ -f "$TEST_PLAN" ]] || { echo "Test plan not found: $TEST_PLAN" >&2; exit 1; }

resolve_jmeter

# Preflight via the pipeline's send-event.sh (a single-shot, end-to-end check),
# so a run never floods a backend whose downstream is broken. --skip-preflight bypasses
# this to deliberately load an ingestion path whose downstream (Flink/MinIO) is unhealthy.
if [[ "$SKIP_PREFLIGHT" == true ]]; then
  echo "→ Preflight skipped (--skip-preflight); loading the backend without a pipeline check."
elif [[ "$BACKEND_HOST" != localhost && "$BACKEND_HOST" != 127.0.0.1 && "$BACKEND_HOST" != "::1" ]]; then
  # send-event.sh posts to localhost and verifies the local pipeline; it cannot reach a remote backend.
  echo "→ Preflight skipped: --backend-host ${BACKEND_HOST} is remote, which send-event.sh cannot validate."
elif [[ ! -x "$PREFLIGHT_SENDER" ]]; then
  echo "✗ Preflight sender not found for --pipeline ${PIPELINE}: $PREFLIGHT_SENDER" >&2
  echo "  Re-run with --skip-preflight to load the backend anyway." >&2
  exit 1
elif ! "$PREFLIGHT_SENDER"; then
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

SAMPLER_PID=""
SAMPLES_FILE=""
if [[ -n "$ADVISE_MINUTES" ]]; then
  require_docker
  advise_container_ids="$(compose ps -q 2>/dev/null || true)"
  [[ -n "$advise_container_ids" ]] || {
    echo "✗ --advise needs the stack running, but no compose containers were found." >&2
    exit 1
  }
  # Kept alongside the JMeter results (not a temp file) so the raw per-sample series survives the run
  # and averages/peaks can be recomputed later. Columns: name<TAB>cpu%<TAB>memUsage<TAB>mem%.
  SAMPLES_FILE="$RESULTS_DIR/$ts-stats.tsv"
  trap '[[ -n "$SAMPLER_PID" ]] && kill "$SAMPLER_PID" 2>/dev/null' EXIT
  SAMPLER_PID="$(start_stats_sampler "$SAMPLES_FILE" "$advise_container_ids")"
  # N is the total observation window measured from here, not a post-load drain — the load run
  # happens inside it, and any time left over is spent watching the pipeline drain.
  ADVISE_WINDOW_START="$(date +%s)"
  echo "→ Sampling docker stats every ${ADVISE_SAMPLE_SECONDS}s over a ${ADVISE_MINUTES} min observation window."
fi

echo
echo "→ Load run: ${THREADS} threads x ${ITERATIONS} iterations = $((THREADS * ITERATIONS)) requests → ${BACKEND_URL}"
"$JMETER_BIN" -n -t "$TEST_PLAN" \
  -Jthreads="$THREADS" -Jiterations="$ITERATIONS" -Jbackend_url="$BACKEND_URL" \
  -l "$jtl" -j "$RESULTS_DIR/$ts.log" -e -o "$report"

echo
echo "✓ Done. Raw results: ${jtl}"
echo "  HTML dashboard:    ${report}/index.html"

if [[ -n "$ADVISE_MINUTES" ]]; then
  remaining=$(( ADVISE_MINUTES * 60 - ( $(date +%s) - ADVISE_WINDOW_START ) ))
  if (( remaining > 0 )); then
    echo
    echo "→ Observing the pipeline for the rest of the ${ADVISE_MINUTES} min window (~${remaining}s left;" \
         "Flink keeps consuming after JMeter stops) …"
    sleep "$remaining"
  elif (( ADVISE_MINUTES > 0 )); then
    echo
    echo "→ The load run alone filled the ${ADVISE_MINUTES} min window; advising on what was sampled."
  fi
  kill "$SAMPLER_PID" 2>/dev/null || true
  wait "$SAMPLER_PID" 2>/dev/null || true
  SAMPLER_PID=""
  advise_tune "$SAMPLES_FILE"
  echo
  echo "  Raw docker-stats samples: ${SAMPLES_FILE}"
fi
