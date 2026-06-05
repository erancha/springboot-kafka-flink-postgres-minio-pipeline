# Real-time Data Pipeline

[![CI](https://github.com/erancha/springboot-kafka-flink-postgres-minio-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/erancha/springboot-kafka-flink-postgres-minio-pipeline/actions/workflows/ci.yml)

**Frontend -> Backend (Spring Boot API) -> Kafka -> Flink -> MinIO/Postgres**

## Summary

The engineering focus of this project is the data path — backend → Kafka → Flink → sinks — and its
failure handling: idempotent upserts, bounded timeouts on every external I/O path, DLQ routing,
exactly-once reasoning, and observability.

The backend is the pipeline's ingestion gate: it validates and normalizes every request before
anything reaches Kafka, and enforces an SSRF (Server-Side Request Forgery) allowlist so
attacker-supplied image URLs can't reach internal hosts — e.g. an `IMAGE` event with
`http://169.254.169.254/` to probe cloud metadata. Doing this at the edge keeps disallowed hosts
out of Kafka entirely, so Flink fetches without re-checking.

Both the data-path handling and the ingestion gate are backed by 100+ tests, including
Testcontainers integration suites; the CI badge above gates the backend and Flink unit tests only,
while the integration and frontend suites are run separately.

### Out of scope

This is an exercise project focused on the data path's failure handling, not a production deployment. The following are deliberately not built:

- **Authentication / authorization** — the ingestion edge (`POST /api/events` and the React UI) is an unauthenticated local tester, not a hardened production boundary: no login, API key, tenant isolation, or rate limiting, with the SSRF allowlist as the only request-level guard.
- **Secrets management & transport security** — credentials are supplied via a gitignored `.env` (only `.env.example`, with blank values, is committed), but there is no Vault / Secrets Manager integration or rotation, and inter-service traffic on the local Docker network is plaintext (no TLS).
- **DLQ operations** — dead-letter records are captured and metered, but not consumed, replayed, or alerted on. See [DLQ operations](docs/PIPELINE_FLOW.md#dlq-operations).
- **High availability** — single Kafka broker (replication factor 1), single Flink JobManager/TaskManager, and unreplicated Postgres/MinIO; any single loss can mean data loss or downtime. See [Out of scope](docs/PIPELINE_FLOW.md#out-of-scope).
- **Paging / alerting** — Grafana dashboards are for inspection only; no Alertmanager is wired to any signal.

## Overview

This project is a [docker-compose](scripts/docker-compose.yml) deployable real-time data
pipeline. Events flow one direction — Frontend → Backend → Kafka → Flink → sinks:

- **Frontend** — minimal tester: submits `IMAGE` / `DATA` events to the API.
- **Backend** — validates and publishes event JSON, and uploads file bytes to
  MinIO. It is a narrow, synchronous validation gate: every request passes through it before
  anything reaches Kafka, rejected with a 4xx if malformed or disallowed. `IMAGE`-by-URL events
  are checked against an SSRF allowlist at this edge — the trust boundary where user-supplied URLs
  enter — so a disallowed host never reaches Kafka and Flink fetches without re-checking (assumes
  the backend is the sole producer to `events`).
- **Kafka** — durable event backbone: a single topic (`events`) keyed by event `id`. Both
  event types share one lifecycle, so one topic keeps the pipeline minimal — splitting would pay
  off only with per-type retention, scaling/ACLs, or ownership boundaries. The `id` (UUID)
  partition key keeps retries/duplicates of a logical event on one partition and preserves
  per-event ordering, while leaving cross-event ordering free for parallelism.
- **Flink** — consumes the stream and routes by event type:
  - `IMAGE` events **always** land in MinIO (`images/{date}/{id}.{ext}`) — whether uploaded as a
    file (stored by the backend at ingestion) or supplied as a URL (fetched and **cloned** into
    MinIO by Flink; only the object key is persisted to Postgres, never the source URL).
    [Why clone rather than reference](docs/PIPELINE_FLOW.md#async-image-enrichment): durable and
    self-contained.
  - `DATA` events are stored in Postgres (`processed_events`).

Everything after Kafka is the Flink job's responsibility; see
[`docs/PIPELINE_FLOW.md`](docs/PIPELINE_FLOW.md).

## Architecture

![Data Pipeline Architecture](docs/architecture.svg)

| Service    | Description                                                                                                                                                                                                                                                                                                                                  | URL / Access                                                                           |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| frontend   | React + Vite, built and served via Nginx                                                                                                                                                                                                                                                                                                     | http://localhost:3030                                                                  |
| backend    | Spring Boot API: [receives](backend/src/main/java/com/webcharm/backend/api/EventController.java) requests, [validates and normalizes](backend/src/main/java/com/webcharm/backend/model/EventRequest.java) input (e.g. `TEXT` → `DATA`), and [publishes](backend/src/main/java/com/webcharm/backend/kafka/EventProducer.java) events to Kafka | http://localhost:8030                                                                  |
| kafka      | Kafka (KRaft mode, i.e. no ZooKeeper) + Kafka UI                                                                                                                                                                                                                                                                                             | http://localhost:8088                                                                  |
| flink      | JobManager / TaskManager + job submitter running the streaming job ([StreamingJob.java](flink/src/main/java/com/webcharm/pipeline/StreamingJob.java), built via [flink/pom.xml](flink/pom.xml))                                                                                                                                              | http://localhost:8081 (commented out by default; to [uncomment](scripts/docker-compose.yml))   |
| minio      | Local S3-compatible object storage; the `images` bucket is created at startup by `minio-init`                                                                                                                                                                                                                                                | Console: http://localhost:9011 (user `minio`, pass `minio123`)                         |
| postgres   | Analytics database simulating a data warehouse                                                                                                                                                                                                                                                                                               | localhost:5432 (db `warehouse`, user `postgres`, pass `postgres`)                      |
| grafana    | Pre-provisioned dashboards: Postgres analytics, and a Flink pipeline-health dashboard (consumer lag, job restarts, checkpoint health, backpressure, DLQ volume by stage, retryable enrichment failures) backed by the Prometheus datasource                                                                                                  | [http://localhost:3031](http://localhost:3031/dashboards) (user `admin`, pass `admin`) |
| prometheus | Scrapes the Flink JobManager/TaskManager metrics reporter (port 9249) so pipeline operability — consumer lag, restarts, backpressure, checkpoints, the custom `dlq_records` (by stage) and `image_retryable_failures` counters — is queryable                                                                                                | http://localhost:9090                                                                  |

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
- Send `DATA`, `TEXT`, or `IMAGE` events (`TEXT` is normalized to `DATA`)

Additional commands:

```bash
./scripts/start.sh --help               # start options (--restart, --rebuild)
./scripts/stop.sh --help                # stop the stack; --remove-volumes removes volumes, --prune-dangling-images also purges build artifacts
./scripts/test.sh --help                # run tests; see docs/TESTING.md for suite details
./scripts/stress.sh --help              # concurrent DATA/IMAGE-url load generator (requires --profile testing for IMAGE)

./scripts/ps.sh                         # show running containers and service URLs
./scripts/logs.sh --help                # stream logs (supports -e/--errors and per-service filtering)
./scripts/compare-images.sh             # compare image counts in MinIO vs PostgreSQL
```

## Testing

Unit, component, and integration test suites and how to run them are documented in [TESTING.md](docs/TESTING.md).

## Analytics Queries

Post-hoc vs. Flink-pre-aggregated queries, the windowing behavior, and how to run them (CLI and Grafana) are documented in [ANALYTICS.md](docs/ANALYTICS.md).
