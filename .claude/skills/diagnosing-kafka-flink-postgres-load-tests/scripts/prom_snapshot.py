#!/usr/bin/env python3
"""Textual snapshot of pipeline load-test metrics from Prometheus.

Default mode pulls the canonical Pipeline Health series over a window, auto-detects
each load run (a contiguous span where events actually flowed into Postgres), and
prints a run timeline plus per-run statistics — the text equivalent of the Grafana
dashboard, suitable for diffing one run against another.

Ad-hoc modes (--query / --range) run arbitrary PromQL so the diagnosis can follow
whatever the snapshot turns up, rather than being limited to the canned series.
"""
import argparse
import json
import math
import sys
import time
import urllib.parse
import urllib.request

# Counters whose in-window increase signals a fault (vs. healthy progress like completed_ckpt).
FAULT_COUNTERS = {"restarts", "failed_ckpt"}

# name -> (PromQL, kind). "gauge" series are summarized by peak+avg over a window;
# "counter" series are cumulative, so a window is summarized by its delta (end-start),
# which answers "did this happen DURING the run?" rather than "ever".
METRICS = {
    "tput_data rec/s":      ('sum(rate(flink_taskmanager_job_task_operator_numRecordsIn{operator_name="data_to_postgres"}[1m]))', "gauge"),
    "tput_image rec/s":     ('sum(rate(flink_taskmanager_job_task_operator_numRecordsIn{operator_name="image_to_postgres"}[1m]))', "gauge"),
    "kafka_lag":            ('sum(kafka_consumergroup_lag{consumergroup="flink-processor",topic="events"})', "gauge"),
    "flink_pending":        ('sum(flink_taskmanager_job_task_operator_pendingRecords)', "gauge"),
    "busy ms/s":            ('avg(flink_taskmanager_job_task_busyTimeMsPerSecond)', "gauge"),
    "backpressure ms/s":    ('avg(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)', "gauge"),
    "ckpt_dur ms":          ('max(flink_jobmanager_job_lastCheckpointDuration)', "gauge"),
    "minio_upload ms":      ('(sum(rate(flink_taskmanager_job_task_operator_minio_upload_nanos[1m])) / sum(rate(flink_taskmanager_job_task_operator_minio_uploads[1m]))) / 1e6', "gauge"),
    "dlq rec/s":            ('sum(rate(flink_taskmanager_job_task_operator_stage_dlq_records[1m]))', "gauge"),
    "retryable/s":          ('sum(rate(flink_taskmanager_job_task_operator_image_retryable_failures[1m]))', "gauge"),
    "restarts":             ('max(flink_jobmanager_job_numRestarts)', "counter"),
    "failed_ckpt":          ('max(flink_jobmanager_job_numberOfFailedCheckpoints)', "counter"),
    "completed_ckpt":       ('max(flink_jobmanager_job_numberOfCompletedCheckpoints)', "counter"),
}

# Throughput floor (rec/s) that counts as "a run is active", and the idle gap (s) that
# separates one run from the next.
ACTIVE_FLOOR = 50
RUN_GAP_SECS = 180


def prom(base, path, params):
    url = f"{base}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def query_range(base, expr, start, end, step):
    r = prom(base, "/api/v1/query_range",
             {"query": expr, "start": start, "end": end, "step": step})
    res = r["data"]["result"]
    return [(int(t), float(v)) for t, v in res[0]["values"]] if res else []


def fmt(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts))


def detect_runs(data):
    """Contiguous spans where total Postgres throughput clears ACTIVE_FLOOR."""
    totals = {}
    for key in ("tput_data rec/s", "tput_image rec/s"):
        for t, v in data.get(key, []):
            totals[t] = totals.get(t, 0.0) + v
    active = sorted(t for t, v in totals.items() if v > ACTIVE_FLOOR)
    runs, start, prev = [], None, None
    for t in active:
        if start is None:
            start = t
        elif t - prev > RUN_GAP_SECS:
            runs.append((start, prev))
            start = t
        prev = t
    if start is not None:
        runs.append((start, prev))
    return runs


def window_stat(series, a, b, kind):
    pts = [(t, v) for t, v in series if a <= t <= b and not math.isnan(v)]
    if not pts:
        return None
    if kind == "counter":
        return ("delta", pts[-1][1] - pts[0][1])
    vals = [v for _, v in pts]
    return ("peak/avg", max(vals), sum(vals) / len(vals))


def snapshot(args):
    base = args.prom_url.rstrip("/")
    end = int(time.time())
    start = end - args.since
    data = {name: query_range(base, expr, start, end, args.step)
            for name, (expr, _) in METRICS.items()}

    runs = detect_runs(data)
    print(f"# Prometheus snapshot  {base}")
    print(f"# window: last {args.since}s  ({fmt(start)}-{fmt(end)} local)  step={args.step}s")
    print(f"# detected {len(runs)} run(s)")
    print("# metric definitions: see SKILL.md 'Metric glossary'\n")

    if not runs:
        print("No active runs in window. Widen --since, or confirm a load test actually ran.")
        return

    for i, (a, b) in enumerate(runs, 1):
        print(f"=== RUN {i}  {fmt(a)}-{fmt(b)} local  ({b - a}s active) ===")
        for name, (_, kind) in METRICS.items():
            s = window_stat(data[name], a, b, kind)
            if s is None:
                print(f"  {name:<20} (no data)")
            elif s[0] == "delta":
                flag = "  <-- FAULT during run" if (s[1] > 0 and name in FAULT_COUNTERS) else ""
                print(f"  {name:<20} delta in window : {s[1]:10.0f}{flag}")
            else:
                print(f"  {name:<20} peak/avg        : {s[1]:10.1f} / {s[2]:10.1f}")
        print()

    if len(runs) >= 2:
        print("# Compare runs: a later run that is SLOWER (higher lag/busy/ckpt) at the SAME")
        print("# throughput, with restarts/failed_ckpt delta = 0, points downstream (sink/DB),")
        print("# not at the pipeline. Correlate with Postgres table growth (diagnose.sh does this).")


def adhoc(args):
    base = args.prom_url.rstrip("/")
    if args.query:
        r = prom(base, "/api/v1/query", {"query": args.query})
        for m in r["data"]["result"]:
            print(json.dumps(m.get("metric", {})), "=>", m["value"][1])
    else:
        end = int(time.time())
        series = query_range(base, args.range, end - args.since, end, args.step)
        for t, v in series:
            print(fmt(t), f"{v:.3f}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--prom-url", default="http://localhost:9090")
    p.add_argument("--since", type=int, default=10800, help="lookback window in seconds (default 3h)")
    p.add_argument("--step", type=int, default=30, help="sample step in seconds")
    p.add_argument("--query", help="ad-hoc instant PromQL (overrides snapshot mode)")
    p.add_argument("--range", help="ad-hoc range PromQL over the window (overrides snapshot mode)")
    args = p.parse_args()
    try:
        if args.query or args.range:
            adhoc(args)
        else:
            snapshot(args)
    except urllib.error.URLError as e:
        print(f"Cannot reach Prometheus at {args.prom_url}: {e}", file=sys.stderr)
        print("The stack must be up. Start it with ./scripts/start.sh", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
