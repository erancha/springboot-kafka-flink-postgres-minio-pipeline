---
name: diagnosing-kafka-flink-postgres-load-tests
description: Use when a load test of this Kafka->Flink->Postgres/MinIO event pipeline shows degraded throughput, rising Kafka consumer lag, growing busy time, slow checkpoints, run-to-run regression, or DLQ growth, and the Docker stack is still up to inspect. Triggers include "second run was slower/busier", "consumer lag spiked", "checkpoints got slow", "events aren't draining".
---

# Diagnosing Kafka -> Flink -> Postgres Load Tests

## Overview

Turn a live (still-running) Docker stack into a textual diagnosis of a load test, instead of
reading Grafana screenshots. The harness collects evidence; **you** correlate it. The dynamic
part is the correlation — the failure mode is rarely the same twice, so this skill gives you the
evidence bundle plus a map from symptom to likely cause and the next probe.

**Core principle:** a load test that *completes* can still be unhealthy. Same end-to-end
throughput with higher lag / busy time / checkpoint duration means a queue got deeper, not that
nothing was wrong. Find what got slower and why.

## Prerequisite

The stack must be up (`./scripts/start.sh`). Every script reads from the running containers and
Prometheus; none of them start, stop, or mutate the stack.

## Step 1 — collect the evidence bundle

Default the lookback to **3h**. If the skill was invoked with an explicit duration
(e.g. `/diagnosing-kafka-flink-postgres-load-tests 5h`, or `90m`), pass that as `--since` instead —
do not silently keep 3h when the user named a window.

```bash
.claude/skills/diagnosing-kafka-flink-postgres-load-tests/scripts/diagnose.sh --since 3h   # or the duration the skill was given
```

Prints five sections: stack status, **run timeline + per-run metrics**, **alerts fired / resolved**,
**Postgres table status**, and **recent error signatures**.

Detection is **windowed**: it only sees runs inside `[now − since, now]`, so any run that ended
before the window started is invisible — it does not scan from the beginning of history. The bundle
prints `window: … detected N run(s)`; if you expected more runs than that, the earlier ones fell
outside the window — widen `--since` (e.g. to cover the stack's full uptime from `docker ps`) and
re-run.

The run timeline auto-detects each load run and reports, per run: throughput, Kafka lag, Flink
pending, busy/backpressure ms/s, checkpoint duration, MinIO upload time, DLQ + retryable rates, and
the **window delta** of restarts / failed / completed checkpoints. A counter delta > 0 means it
*happened during that run* — distinguishing a real fault from a startup-leftover counter value.

When the window holds a single uninterrupted run, it is auto-split into 10 equal time slices
reported as `SLICE 1/10 … 10/10`. A lone run has no later run to diff against, so this restores the
trend view — intra-run drift (e.g. insert latency climbing as the table grows) shows up as rising
lag/busy/ckpt across slices at the same throughput. Narrow `--since` to bracket one run if the
window caught several and you want the per-slice view of just one.

## Alerts fired / resolved

The alerts section lists each Grafana alert episode in the window, one line per fired→resolved
pair, anchored to the fire time so it lines up with the run windows above it:

```
FIRED 15:19:10   RESOLVED 15:21:10  (2m00s)   [Error]    Flink restart looping  | host_machine=Eran, severity=warning
FIRED 19:47:10   RESOLVED — still active      [Pending]  Kafka consumer stalled | host_machine=Eran, severity=critical
```

Alerts are Grafana-managed (rules in `infra/grafana/provisioning/alerting/`, e.g.
`flink-alerts.yaml` / `backend-alerts.yaml`), not
Prometheus — Prometheus carries no alerting rules here, so there is no `ALERTS` series to read.
Grafana records every state transition (Normal → Pending → Alerting/Error → Normal) in its SQLite
store; `diagnose.sh` copies that db out of the container read-only and `grafana_alerts.py` pairs
each departure from Normal with the next return to Normal. No Grafana credentials are involved.

Reading the line:

- `[state]` is the **peak** state the episode reached. `Pending` = the rule's condition was true but
  cleared before its `for` duration elapsed (a near-miss that never notified). `Alerting` = it
  actually fired. `Error` / `NoData` = Grafana could not evaluate the rule (the DatasourceError /
  missing-series states). An episode that only ever reached `Pending` is informational, not a firing.
- `RESOLVED — still active` means the alert had not returned to Normal by the end of the window.
- `FIRED <before window>` means the fire transition predates the window start; only the resolve was
  captured. Widen `--since` to see when it fired.
- Times are **local**, already converted from the stored epochs — unlike the *recent error
  signatures* section, which is UTC. Correlate a fire time directly against the run windows; an alert
  that fires inside a run window is a load-induced signal, one that fires between runs is not.

## Metric glossary — what each row measures, and where

Every metric is one PromQL series in `prom_snapshot.py`; "where" is the point in the pipeline it is
sampled, which disambiguates the name (e.g. throughput is sampled at the **Postgres-writing sink**,
not at Kafka ingestion — see `tput_data`).

| Metric | What it measures | Where in the pipeline |
|---|---|---|
| `tput_data rec/s` | DATA records/s *entering* the `data_to_postgres` sink (`rate(numRecordsIn)`). The write end, **not** Kafka ingestion. Tracks the Postgres write rate closely (the sink backpressures when Postgres is slow) but counts records the sink *received*, not rows confirmed committed — an in-flight JDBC batch is already counted | Flink sink input, just before the JDBC write |
| `tput_image rec/s` | Same as `tput_data`, for the `image_to_postgres` sink (`rate(numRecordsIn)`) | Flink sink input, just before the JDBC write |
| `kafka_lag` | Committed consumer lag = latest offset − committed offset on `events` (from kafka-exporter) | Kafka's view of how far Flink's *committed* offset trails the producer |
| `flink_pending` | Records fetched from Kafka but not yet processed (`pendingRecords`); reported only while the source is RUNNING | Flink Kafka source, internal backlog |
| `busy ms/s` | ms per second each task was actively processing, **including blocking I/O** like a JDBC batch (0–1000, avg across tasks) | All Flink operators |
| `backpressure ms/s` | ms per second a task stalled waiting on a downstream buffer (0–1000) | All Flink operators |
| `ckpt_dur ms` | Wall-clock duration of the last completed checkpoint | Flink JobManager (waits on in-flight sink flush) |
| `minio_upload ms` | Mean per-image upload latency (`upload_nanos / uploads`) | IMAGE URL-enrichment path → MinIO |
| `dlq rec/s` | Records routed to the dead-letter sink per second, by stage | Flink dead-letter path |
| `retryable/s` | Retryable enrichment failures per second (URL fetch / MinIO) | IMAGE enrichment |
| `restarts` (counter) | Cumulative Flink job restarts; **window delta** = restarts during the run | Flink JobManager |
| `failed_ckpt` (counter) | Cumulative failed checkpoints; window delta = failures during the run | Flink JobManager |
| `completed_ckpt` (counter) | Cumulative completed checkpoints; window delta = healthy progress during the run | Flink JobManager |

## Step 2 — tabulate the runs first

Before correlating anything, transcribe the per-run metrics from the bundle into a single
comparison table — one column per run, one row per metric — so run-to-run drift is visible at a
glance instead of buried in the separate per-run text blocks. **Label each run column with its
start–end time in parentheses**; that window is what every counter delta and every Postgres/Kafka
log line gets correlated against, so it must travel with the numbers.

| Metric (avg) | Run 1 (17:23:43–17:42:43) | Run 2 (18:02:43–18:21:13) | Run 3 (18:43:13–19:06:43) |
|---|---|---|---|
| tput_data rec/s | … | … | … |
| kafka_lag | … | … | … |
| flink_pending | … | … | … |
| busy ms/s | … | … | … |
| ckpt_dur ms | … | … | … |
| restarts / failed_ckpt delta | … | … | … |

Read the table top-to-bottom for the trend — is a later run slower (higher lag/busy/ckpt) at the
*same* throughput? — before dropping into the symptom map. The timestamps in the headers are the
windows you hand to every log and Prometheus probe that follows.

When the bundle reports `SLICE n/10` instead of runs (a single continuous run), tabulate the 10
slices as the columns and read left-to-right — same method, finer grain: rising lag/busy/ckpt across
slices at flat throughput is the table deepening its own backlog within one run.

## Step 3 — read the signals

```
                       errors during a run window?
   restarts/failed_ckpt delta > 0 ──yes──> real fault: pull flink-* logs + DLQ; check whether a
              │                             slow batch tripped the JDBC timeout budget (see below)
              no
              │
   degraded but throughput ~unchanged?
   busy HIGH + backpressure ~0 + lag rising ──yes──> sink/DB-bound (operator blocks inside JDBC).
              │                                       Go to Postgres table status.
              no ──> source/producer-bound: check backend rate, Kafka controller health.
```

| Signal in the bundle | Likely cause | Next probe |
|---|---|---|
| Run N slower than identical run N-1 (higher lag/busy/ckpt), same throughput, zero error deltas | Table never truncated between runs; heap+indexes outgrow `shared_buffers`, random-UUID PK scatters inserts → cold-page I/O | Postgres table status: `total`/`indexes` size vs `shared_buffers`. Truncate between runs to get comparable numbers |
| busy ms/s high, backpressure ~0, lag climbing | Sink blocks inside a slow JDBC batch (busy counts blocking I/O); no upstream backpressure signal | Postgres size + `last_autovacuum`; was autovacuum/analyze competing during the run |
| checkpoint duration climbing across the run | Checkpoint waits on in-flight sink flush; same root as slow inserts | Postgres insert latency / table size |
| flink-taskmanager log `Thread starvation or clock leap detected` (Hikari housekeeper delta far exceeds its interval), often paired with a jobmanager checkpoint `AskTimeoutException` | TaskManager JVM starved (long GC / CPU contention under load) or the WSL2 VM was paused (host sleep / memory pressure); the checkpoint trigger RPC then times out because the TM thread can't answer. The checkpoint failure is a symptom, not the root — and this can be an environment artifact, not a pipeline bug | `FLINK_TM_MEMORY` + TM GC pauses; host/VM memory and whether the VM was paused; confirm restarts delta = 0 (Flink usually absorbs a single occurrence) |
| postgres log `connection to client lost`, clustered in the slow run | A degraded batch crossed `socketTimeout`/`queryTimeout` (default 20s, `JDBC_*_TIMEOUT_SECS`); driver dropped the socket → sink reconnect+retry | `flink/.../sinks/JdbcWriterBase.java` retry/classification; confirm a batch can exceed the timeout under the run's insert latency |
| kafka log `controller event queue overloaded` / `REQUEST_TIMED_OUT`, bursts of `NotCoordinator` / `CoordinatorLoadInProgress` offset-commit WARNs | Single-node KRaft broker+controller starved under load; offset commits are retriable and ride the next checkpoint — noise unless paired with restarts | Confirm restarts delta = 0; if so, benign. Otherwise raise Kafka memory/heartbeat headroom |
| dlq rec/s or retryable/s > 0 | Permanent failures (poison rows) or enrichment retries | DLQ-by-stage breakdown; for IMAGE, MinIO upload time + allowlist/SSRF 403s |
| MinIO upload time spiking | URL-fetch enrichment slow or MinIO saturated | MinIO container memory/health; upstream URL latency |

## Step 4 — knobs (where they live)

- **Test hygiene first.** `TRUNCATE processed_events, ...` before each measured run (see
  `./scripts/sql-helper.sh -h`). Without it, run N is slower than run N-1 by construction.
- `.env`: `PIPELINE_PARALLELISM` (couples topic partitions + Flink slots + Postgres connections),
  `JDBC_BATCH_SIZE`, `JDBC_*_TIMEOUT_SECS` / `JDBC_MAX_ATTEMPTS` (retry budget must stay under the
  60s checkpoint timeout), `FLINK_TM_MEMORY`.
- `scripts/docker-compose.yml`: Postgres `shared_buffers` / `max_wal_size` / `max_connections`,
  Kafka and Postgres memory caps.
- Schema (real insert-throughput hardening for an unbounded table): monotonic PK instead of random
  UUIDv4, fewer secondary indexes during load, or an `UNLOGGED` table for throwaway demos.

## Ad-hoc PromQL

When the bundle raises a new question, query Prometheus directly instead of guessing:

```bash
S=.claude/skills/diagnosing-kafka-flink-postgres-load-tests/scripts/prom_snapshot.py
python3 $S --query 'sum(kafka_consumergroup_lag{consumergroup="flink-processor"})'
python3 $S --range 'avg(flink_taskmanager_job_task_busyTimeMsPerSecond)' --since 1800
```

## Common mistakes

- **Calling a completed run healthy.** Check lag/busy/ckpt depth, not just "it finished".
- **Comparing runs without truncating between them.** The later run is slower by construction.
- **Treating `NotCoordinator` offset-commit WARNs as failures.** They are retriable and benign
  unless a restart delta accompanies them.
- **Reading `max()` counter values as per-run.** Use the window *delta* the timeline reports.
- **Mixing UTC and local time when correlating.** Container service logs — the *recent error
  signatures* section, and any raw `docker logs` / Flink / Kafka / Postgres output — are timestamped
  in **UTC**, while the run-timeline windows, `docker ps` uptimes, and your shell `date` are
  **local**. Convert before matching a log line to a run window: a Flink `18:13:45` log is
  `21:13:45` in a UTC+3 local timeline (and would otherwise look like it predates a run it was
  actually inside). Compute the offset once — `date` against any fresh UTC log line — and apply it
  to every error-signature timestamp.
