# Event Pipeline Tests

How the DATA/IMAGE pipeline is covered across the [testing layers](../TESTING.md), and how to
load-test its ingestion path. Run the suites with the commands documented in that page.

- **Unit** — Flink operators and backend services exercised in isolation, all dependencies mocked (parse/route, async image enrichment, the JDBC writers, Kafka publish, object-store upload).
- **Component** — the backend web layer via Spring MockMvc and the React UI in jsdom: the framework is loaded but the broker, object store, and database are mocked.
- **Integration** — vertical slices against real infrastructure: Flink mini-clusters for the windowed and image pipelines, and a throwaway Testcontainers Postgres for the JDBC writer.

## Load testing — ingestion vs. drain

**Three roles, ideally on three separate machines:**

- **A — Data pipeline:** the Docker stack (Kafka → Flink → Postgres/MinIO).
- **B — Backend:** the Spring Boot ingestion service plus its own Prometheus + Grafana, pointed at A's broker across the network.
- **C — JMeter:** the load generator, flooding B with HTTP events.

C should stay off B: the load generator competes with the backend for CPU, so co-locating them
caps ingestion. The readings below were taken with **C running on B** (one machine short), so read
the ingestion ceiling as load-generator-bound, not a true backend limit.

```bash
# on B — backend (+ Prometheus + Grafana) against A's broker:
./scripts/start-backend.sh --kafka-host <A-ip>

# on C — drive the load at B across the network (omit --backend-host when C runs on B):
./scripts/jmeter-helper.sh --backend-host <B-ip> -t 500 -i 20000   # 500 clients × 20k = 10M DATA requests
```

`jmeter-helper.sh` marks a request done the instant B publishes it and Kafka acks — nothing
downstream. Its throughput and latencies therefore describe B's acceptance edge alone; whether A
keeps up with that inflow is the separate question the drain rate answers.

**Where to read the numbers:**

- Ingestion throughput / latency / errors → JMeter's run report on C, under `jmeter/results/`.
- Backend CPU & heap → the *Backend Ingestion* Grafana dashboard on B.
- Drain rate → the *Events/sec into Postgres* panel on A's Grafana.

A representative run (C co-located on B):

| Metric | Value |
| --- | --- |
| Workload | 500 concurrent clients, 10M requests; B and C share one host, separate from A |
| Sustained ingestion | ~10K req/s over ~40 min |
| Errors | 0 / 10,000,000 (0.00%) |
| Latency p50 / p95 / p99 / p99.9 | 112 / 199 / 270 / 526 ms |
| Latency avg / max | 119 / 3,565 ms |
| Backend footprint | 1 GiB heap cap, ~320 MiB peak; CPU peaked ~2.75 cores |
| Saturating component | Kafka broker on A (~80% CPU); B stayed near-idle |

**What it shows:** A's Kafka broker saturates first (~80% CPU); B stays near-idle, and A's Flink
drains to Postgres *faster* than B ingests — so neither the backend nor the sink is the bottleneck.
The two rates are measured at different stages:

- **~10K req/s ingestion** — bounded by the shared B+C host (they contend for CPU) plus the B → A network hop, not by A.
- **~12K events/s drain** — A's Flink write rate into Postgres, which keeps burning down Kafka lag
  even after C stops; the [batched Postgres sink](../batched-jdbc-sink-tradeoffs.md) outruns single-host ingestion.
