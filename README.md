# Real-time Data Pipeline

## UI -> Spring Boot API -> Kafka -> Flink -> MinIO/Postgres

## Overview

This project is a local, Docker Compose–based real-time data pipeline:

- UI (React) sends events to the API
- API (Spring Boot) validates requests and publishes JSON events to Kafka
- Flink consumes Kafka events and processes them:
  - `IMAGE` events are stored in MinIO at `images/{date}/{id}.jpg`
  - `DATA` events are cleaned/normalized and stored in Postgres (`processed_events`)

## Architecture

![Data Pipeline Architecture](docs/architecture.svg)

- **frontend**: React + Vite built and served via Nginx
  - URL: http://localhost:3030
- **backend**: Spring Boot API that [receives](backend/src/main/java/com/webcharm/backend/api/EventController.java) requests, [validates](backend/src/main/java/com/webcharm/backend/model/EventRequest.java) input, and [publishes](backend/src/main/java/com/webcharm/backend/kafka/EventProducer.java) events to Kafka
  - URL: http://localhost:8030
- **kafka**: Kafka (KRaft mode, i.e. no ZooKeeper) + Kafka UI
  - URL: http://localhost:8088
- **flink**: Flink JobManager / TaskManager plus job submitter ([docker-compose.yml](docker-compose.yml)) that runs the streaming job ([StreamingJob.java](flink/src/main/java/com/webcharm/pipeline/StreamingJob.java), built via [flink/pom.xml](flink/pom.xml))
  - URL: http://localhost:8081 (port commented out by default — uncomment `ports` in docker-compose.yml to expose)
- **minio**: local S3-compatible object storage
  - Console: http://localhost:9011 (user: `minio`, pass: `minio123`)
- **postgres**: analytics database simulating a data warehouse
  - Conn: localhost:5432 (db: `warehouse`, user: `postgres`, pass: `postgres`)
- **grafana**: dashboards for Postgres analytics (pre-provisioned)
  - URL: http://localhost:3031 (user: `admin`, pass: `admin`)

## Getting Started

Prereqs:

- Docker Desktop
- Docker Compose v2

WSL:

- Run the scripts from a WSL shell (Bash)

```bash
# If needed, make scripts executable:
chmod +x scripts/*.sh

./scripts/start.sh
```

Then:

- Open UI at http://localhost:3030
- Send `DATA` or `IMAGE` events

Additional commands:

```bash
./scripts/start.sh --help               # start options (--restart, --rebuild)
./scripts/stop.sh --help                # stop the stack; --remove-volumes removes volumes, --prune-dangling-images also purges build artifacts
./scripts/test.sh --help                # run tests; see next section for suite details

./scripts/ps.sh                         # show running containers and service URLs
./scripts/logs.sh --help                # stream logs (supports -e/--errors and per-service filtering)
./scripts/compare-images.sh             # compare image counts in MinIO vs PostgreSQL
```

## Testing

### Unit tests — no Docker, no framework startup

Pure unit tests: exercise one class in isolation, all dependencies mocked.

```bash
mvn -f flink/pom.xml test
```

- `ParseEventFunction` — JSON parsing and DLQ side-output routing for bad payloads
- `MinioUploadFunction` — SSRF URL-validation, statObject existence guard, DLQ routing, HTTP response-size cap, fetch retry logic
- `PostgresProcessedEventWriter` — JDBC parameter binding and error semantics via a mock `Connection`
- `EventProducer` (backend) — Kafka publish and error handling; run via `mvn -f backend/pom.xml test`
- `MinioUploadService` (backend) — upload path and exception wrapping; run via `mvn -f backend/pom.xml test`

### Component tests — no Docker, but load a framework or browser context

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

### Integration tests — require Docker (~60 sec)

```bash
mvn -f flink/pom.xml verify
```

- `PostgresProcessedEventWriterIT` — Testcontainers starts a throwaway `postgres:16` container (not the application stack) solely for the test, then tears it down automatically. Verifies the upsert SQL, JSONB handling, and `ON CONFLICT` deduplication against a live database.
- `StreamingJobIT` — runs the windowed count pipeline in a Flink mini-cluster (in-process, no Docker) against a bounded source and asserts output records.

Also reruns all Flink unit tests.

> First run pulls the `postgres:16` Docker image (~150 MB).

## Analytics Queries

All analytics queries are defined in [samples/analytics.sql](samples/analytics.sql) and query PostgreSQL. They differ in _when_ they are computed:

**Post-hoc analytics** (computed at query time):

- Count events by type
- Retrieve latest records
- Aggregate by hour

These scan the `processed_events` table and run standard SQL aggregations. Flink is not involved.

**Real-time analytics** (pre-aggregated by Flink):

- 5-minute tumbling-window event count per `eventType` (stored in `event_type_counts_5m`)
- Flink computes continuously; queries read pre-computed results (no query-time latency)

**Flink windowing behavior:**

The 5-minute tumbling-window aggregation ([StreamingJob.java:104-124](flink/src/main/java/com/webcharm/pipeline/StreamingJob.java)) uses Flink's default behavior: it only emits window results for windows that contain at least one event. Empty windows are not materialized. This means the `event_type_counts_5m` table will only have rows for time periods when events actually arrived. If there are no `DATA` events in a 5-minute window, that window will not appear in the results, even if the same period had `IMAGE` events. This is standard Flink behavior and conserves storage; to include all windows (including empty ones), the job would need explicit late-firing or allowed lateness policies.

### Running the queries:

Via CLI:

```bash
./scripts/sql-file.sh samples/analytics.sql
```

Via Grafana dashboard:
Access [http://localhost:3031](http://localhost:3031/d/processed-events/processed-events-analytics?orgId=1&refresh=10s) (user: `admin`, pass: `admin`), then:

- Dashboards → Browse → **Processed Events Analytics**

Both CLI and Grafana execute the same SQL against PostgreSQL; the difference is presentation (one-off results vs. live dashboard). The "Flink pre-aggregated" panel reads from `event_type_counts_5m`, while the others query `processed_events` at query time.

![Grafana dashboard screenshot](docs/Grafana.jpg)

## Notes / decisions

- Events are serialized as JSON strings in Kafka. (To view events: see [Architecture](#architecture) and open Kafka UI topic `events`.)
- A single Kafka topic (`events`) is used to keep the pipeline minimal and because both event types share the same lifecycle (ingest -> process -> sink). It makes sense to split topics when:
  - You need different retention/compaction policies per event type.
  - You want independent scaling/quotas/ACLs per event type (e.g. high-volume `IMAGE` vs low-volume `DATA`).
  - Different teams/consumers own different event streams and you want isolation.
- The Kafka partition key is the event `id` (UUID) so all retries/duplicates for the same logical event are consistently routed to the same partition, and ordering is preserved for that key. Across different ids, ordering is intentionally not guaranteed (to allow parallelism).
- Backend validation vs Flink validation:
  - The backend validates request structure and basic constraints (e.g. required fields like `eventType`) before publishing.
  - The backend can also do lightweight checks for `IMAGE` URLs (e.g. non-empty, valid URL syntax, allowed schemes), but it should avoid heavy validation (fetching the URL, checking content-type/size) because that adds latency, can be flaky, and couples ingestion to external availability.
  - Flink is responsible for runtime validation/handling during processing (e.g. attempting to fetch the image, dealing with HTTP failures/timeouts, and deciding whether to drop/route to a DLQ if you add one).
- The Flink job uses routing based on `eventType` and writes to two different sinks (Postgres for `DATA`, MinIO for `IMAGE`).
- MinIO bucket `images` is created by `minio-init` on startup. (To browse stored images: see [Architecture](#architecture).)

## Error handling and fault tolerance

The pipeline achieves **effective exactly-once** delivery: Flink checkpoints every 10 s; on restart it replays from the last successful checkpoint; idempotent upserts (`ON CONFLICT (id) DO UPDATE` in Postgres, `statObject` existence check in MinIO) absorb any duplicates. Not 2PC.

**What is in place:**

- Kafka consumer offsets are committed only on successful checkpoint (`enable.auto.commit=false`), so the committed offset always reflects durably processed state.
- Flink checkpoints: 10 s interval, 60 s timeout, 5 s minimum pause between checkpoints, one in-flight at a time. Externalized checkpoints (`RETAIN_ON_CANCELLATION`) are kept on disk after cancellation.
- Restart strategy: exponential back-off (5 s initial, 2× multiplier, 10 min cap), indefinite retries.
- Sink write failures propagate as `IOException`, causing Flink to replay from the last checkpoint rather than silently drop records.
- JDBC connections use a HikariCP pool (pool size 1 per task slot, keepalive every 60 s), preventing silent connection death from TCP timeouts or Postgres idle-connection culling.
- Transient Postgres write errors (connection reset, deadlock) are retried in-operator with exponential backoff before escalating to checkpoint replay. Permanent errors (constraint violations, invalid JSONB) are not retried.
- Unparsable events and MinIO upload failures are routed to the `events-dlq` Kafka topic instead of crashing the job.

**Out of scope (single-node deployment):**

- Kafka: single broker, replication factor 1 — broker loss means data loss.
- Flink: single JobManager (no HA), single TaskManager — no automatic failover at the cluster level.
- Postgres and MinIO: no replication or standby.
- Permanent Postgres write failures (e.g. constraint violations) currently trigger checkpoint replay rather than DLQ routing — replay cannot fix a bad payload and will loop until the restart strategy exhausts retries.

See [`flink/PIPELINE_FLOW.md`](flink/PIPELINE_FLOW.md) for the per-failure-mode inventory.
