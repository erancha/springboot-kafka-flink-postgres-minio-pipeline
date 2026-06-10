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

```bash
./scripts/jmeter-helper.sh -t 500 -i 20000   # 500 concurrent clients × 20k = 10M requests
```

`jmeter-helper.sh` floods `POST /api/events` with unique `DATA` events and asserts a 2xx per
request. It measures **only** the synchronous HTTP ingestion edge — not the downstream Flink/MinIO
path. Latencies below are the client-observed HTTP round-trip (JMeter client → backend → 2xx), which
internally covers request validation and a synchronous Kafka publish-and-ack — and, in this run, a
real network hop to the broker.

The headline run used two machines: the backend and JMeter on one host (backend launched with
[`run-backend-local.sh`](../scripts/run-backend-local.sh) against the remote broker), and the Docker
stack — Kafka, Flink, Postgres, MinIO — on another. So backend → Kafka crossed the network, while the
JMeter → backend hop stayed local to the load-generating machine.

| Metric | Value |
| --- | --- |
| Workload | 500 concurrent clients, 10M requests; backend + JMeter on a separate machine from the broker/stack |
| Sustained ingestion | ~10K req/s over ~40 min |
| Errors | 0 / 10,000,000 (0.00%) |
| Latency p50 / p95 / p99 / p99.9 | 112 / 199 / 270 / 526 ms |
| Latency avg / max | 119 / 3,565 ms |
| Backend footprint | 1 GiB heap cap, ~320 MiB peak; CPU peaked ~2.75 cores |
| Saturating component | Kafka broker (~80% CPU); backend stayed near-idle |

The **ingestion rate and the sink-drain rate are measured at different stages**, and
the difference is intentional, not an inconsistency:

- **~10K req/s** is HTTP throughput at the backend edge, bounded by the load-generating machine
  (JMeter and the backend share it) plus the network hop to the broker — a client-side limit, not a
  stack limit.
- **~12K events/s** is Flink's measured write rate into Postgres (Grafana *Events/sec into Postgres*
  panel) as it drains accumulated Kafka backlog. Because Kafka decouples produce from consume, this
  rate holds even after producers stop — the consumer-group lag burns down at ~12K/s with no client
  attached. It demonstrates the [batched Postgres sink](batched-jdbc-sink-tradeoffs.md) outruns
  single-host ingestion, so the sink is not the bottleneck.

## Alerting end-to-end check — requires the running stack

```bash
./scripts/alert-test.sh
```

Unlike the hermetic suites above, this runs against the live stack started by `start.sh`. It stops the Flink TaskManager to induce a real outage, polls Grafana's rule-state API until the `Flink target down` rule reaches `firing`, then restarts the TaskManager and waits for it to clear. Verifies the alert path — Flink reporter → Prometheus → Grafana rule evaluation — while email delivery itself is not asserted.

> Takes the pipeline down for ~2 minutes, and if SMTP is configured in `.env`, firing dispatches a real alert email.
