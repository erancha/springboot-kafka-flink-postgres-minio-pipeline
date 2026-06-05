# Real-time Data Pipeline — Kafka + Flink

[![CI](https://github.com/erancha/springboot-kafka-flink-postgres-minio-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/erancha/springboot-kafka-flink-postgres-minio-pipeline/actions/workflows/ci.yml)

[Summary](#summary) · [Overview](#overview) · [Architecture](#architecture) · [Getting Started](#getting-started) · [Testing](#testing) · [Analytics](#analytics)

## Summary

The engineering focus of this project is the data path — **frontend → backend → Kafka → Flink → sinks** — and its
failure handling: idempotent upserts, bounded timeouts on every external I/O path, DLQ routing,
exactly-once reasoning, and observability.

The backend is the pipeline's ingestion gate: it validates and normalizes every request before
anything reaches Kafka, and enforces an SSRF (Server-Side Request Forgery) allowlist so
attacker-supplied image URLs can't reach internal hosts.

Both the data-path handling and the ingestion gate are backed by 100+ tests, including
Testcontainers integration suites; the CI badge above gates the backend and Flink unit tests only,
while the integration and frontend suites are run separately.

### Out of scope

This is an exercise project focused on the data path's failure handling, not a production deployment. The following are deliberately not built:

- **Authentication / authorization** — the ingestion edge (`POST /api/events` and the React UI) is an unauthenticated local tester, not a hardened production boundary: no login, API key, tenant isolation, or rate limiting, with the SSRF allowlist as the only request-level guard.
- **Secrets management & transport security** — credentials are supplied via a gitignored `.env`, but there is no Vault / Secrets Manager integration or rotation, and inter-service traffic on the local Docker network is plaintext (no TLS).
- **DLQ operations** — dead-letter records are captured and metered, but not consumed, replayed, or alerted on. See [DLQ operations](docs/PIPELINE_FLOW.md#dlq-operations).
- **High availability** — single Kafka broker (replication factor 1), single Flink JobManager/TaskManager, and unreplicated Postgres/MinIO; any single loss can mean data loss or downtime. See [Out of scope](docs/PIPELINE_FLOW.md#out-of-scope).
- **Production paging** — basic Flink-health email alerts exist (see [Observability](docs/PIPELINE_FLOW.md#observability)), but not on-call escalation, Alertmanager-grade silencing/inhibition, or alerting on checkpoints, consumer lag, or the DLQ.

## Overview

This project is a [docker-compose](scripts/docker-compose.yml) deployable real-time data
pipeline. Events flow one direction — Frontend → Backend → Kafka → Flink → sinks:

- **Frontend** (http://localhost:3030) — minimal tester: submits `IMAGE` / `DATA` events to the API.
- **Backend** (http://localhost:8030) — validates and publishes event JSON, and uploads file bytes to
  MinIO. It is a narrow, synchronous [validation gate](backend/src/main/java/com/webcharm/backend/api/EventController.java): every request passes through it before
  anything reaches Kafka, rejected with a 4xx if malformed or disallowed. `IMAGE`-by-URL events
  are checked against an SSRF allowlist at this edge — the trust boundary where user-supplied URLs
  enter — so a disallowed host (e.g. `http://169.254.169.254/`, the cloud-metadata endpoint) never
  reaches Kafka and Flink fetches without re-checking (assumes
  the backend is the sole producer to `events`).
- **Kafka** (localhost:9092) — durable event backbone: a single topic (`events`) keyed by event `id`. Both
  event types share one lifecycle, so one topic keeps the pipeline minimal — splitting would pay
  off only with per-type retention, scaling/ACLs, or ownership boundaries. The `id` (UUID)
  partition key keeps retries/duplicates of a logical event on one partition and preserves
  per-event ordering, while leaving cross-event ordering free for parallelism.
- **Flink** (http://localhost:8081, disabled by default) — consumes the stream and routes by event type:
  - `IMAGE` events **always** land in MinIO (`images/{date}/{id}.{ext}`) — whether uploaded as a
    file (stored by the backend at ingestion) or supplied as a URL (fetched and **cloned** into
    MinIO by Flink; only the object key is persisted to Postgres, never the source URL).
    [Why clone rather than reference](docs/PIPELINE_FLOW.md#async-image-enrichment): durable and
    self-contained.
  - `DATA` events are stored in Postgres.

Everything after Kafka is the Flink job's responsibility
([`StreamingJob.java`](flink/src/main/java/com/webcharm/pipeline/StreamingJob.java)); see
[`docs/PIPELINE_FLOW.md`](docs/PIPELINE_FLOW.md).

## Architecture

![Data Pipeline Architecture](docs/architecture.svg)

The pipeline services and their URLs are described in the [Overview](#overview); the supporting services:

| Service | Role | Access |
| --- | --- | --- |
| Kafka UI | inspect topics, consumer groups | http://localhost:8088 |
| MinIO | object storage; `minio-init` creates the `images` bucket at startup | Console: http://localhost:9011 (`minio` / `minio123`) |
| PostgreSQL | analytics warehouse simulation (processed events + aggregates) | localhost:5432 (`warehouse`, `postgres` / `postgres`) |
| Grafana | Postgres-analytics and Flink pipeline-health dashboards, plus email alert rules on Flink health | [http://localhost:3031](http://localhost:3031/dashboards) (`admin` / `admin`) |
| Prometheus | scrapes Flink metrics for operability | http://localhost:9090 |

## Getting Started

Requires Docker Desktop and Docker Compose v2. Run the Bash scripts from any Linux shell (WSL included):

```bash
chmod +x scripts/*.sh   # if not already executable
./scripts/start.sh
```

Then open the [UI tester](#overview) and send events.

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

## Analytics

Post-hoc vs. Flink-pre-aggregated queries, the windowing behavior, and how to run them (CLI and Grafana) are documented in [ANALYTICS.md](docs/ANALYTICS.md).
