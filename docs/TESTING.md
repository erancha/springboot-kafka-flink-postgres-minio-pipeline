# Testing

## Unit tests — no Docker, no framework startup

Pure unit tests: exercise one class in isolation, all dependencies mocked.

```bash
mvn -f flink/pom.xml test
```

- `ParseEventFunction` — JSON parsing and DLQ side-output routing for bad payloads
- `MinioAsyncImageFunction` — async image fetch + MinIO upload, SSRF guard, DLQ routing, HTTP response-size cap
- `PostgresProcessedEventWriter` — JDBC parameter binding and error semantics via a mock `Connection`
- `EventProducer` (backend) — Kafka publish and error handling; run via `mvn -f backend/pom.xml test`
- `MinioUploadService` (backend) — upload path and exception wrapping; run via `mvn -f backend/pom.xml test`

## Component tests — no Docker, but load a framework or browser context

**Backend** (~35 sec):

```bash
mvn -f backend/pom.xml test
```

`EventController` — Spring MockMvc tests. Loads the web layer (controller + validation) but mocks Kafka and MinIO; no real database or broker.

> Slower than unit tests because Spring builds a partial application context.

**Frontend** (~50 sec):

```bash
cd frontend && npm test && cd ..   # single run
cd frontend && npm run test:watch  # watch mode (re-runs on save)
```

Renders the React `App` component inside jsdom (a simulated browser). Tests cover submit-gate logic (`canSubmit`) and `submit()` error handling.

> Slow because jsdom initialisation takes ~50 sec on first run.

## Integration tests — require Docker (~60 sec)

```bash
mvn -f flink/pom.xml verify
```

- `PostgresProcessedEventWriterIT` — Testcontainers starts a throwaway `postgres:16` container (not the application stack) solely for the test, then tears it down automatically. Verifies the upsert SQL, JSONB handling, and `ON CONFLICT` deduplication against a live database.
- `StreamingJobIT` — runs the windowed count pipeline in a Flink mini-cluster (in-process, no Docker) against a bounded source and asserts output records.
- `ImagePipelineIT` — runs the async image-enrichment pipeline (`buildImagePipeline`) in a Flink mini-cluster and asserts success → main output vs. failure → DLQ side output.

Also reruns all Flink unit tests.

> First run pulls the `postgres:16` Docker image (~150 MB).

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
  even after C stops; the [batched Postgres sink](batched-jdbc-sink-tradeoffs.md) outruns single-host ingestion.

## Alerting end-to-end check — requires the running stack

```bash
./scripts/alert-test.sh
```

Unlike the hermetic suites above, this runs against the live stack started by `start.sh`. It stops the Flink TaskManager to induce a real outage, polls Grafana's rule-state API until the `Flink target down` rule reaches `firing`, then restarts the TaskManager and waits for it to clear. Verifies the alert path — Flink reporter → Prometheus → Grafana rule evaluation — while email delivery itself is not asserted.

> Takes the pipeline down for ~2 minutes, and if SMTP is configured in `.env`, firing dispatches a real alert email.
