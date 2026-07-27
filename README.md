# Real-time Data Pipeline — Kafka + Flink

[![CI](https://github.com/erancha/springboot-kafka-flink-postgres-minio-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/erancha/springboot-kafka-flink-postgres-minio-pipeline/actions/workflows/ci.yml)

A [docker-compose](scripts/docker-compose.yml)-deployable real-time data pipeline: events flow one
direction through **Spring Boot → Kafka → Flink → PostgreSQL / MinIO** sinks.

[Summary](#summary) · [Overview](#overview) · [Architecture](#architecture) · [Getting Started](#getting-started) · [Testing](#testing) · [Analytics](#analytics) · [License](#license)

## Summary

The engineering focus of this project is the data path — [ **frontend →** ] **backend → Kafka → Flink → sinks** — and its
failure handling: idempotent upserts, bounded timeouts on every external I/O path, DLQ routing,
exactly-once reasoning, and observability.

The stack runs **two independent pipelines** over this path, one at a time: **eventtype** (the default —
`IMAGE` / `DATA` events landing in MinIO and PostgreSQL) and **userKeys** (`{userId, key, value}`
events summed by key over event-time windows into PostgreSQL).

The backend is the pipeline's ingestion gate: it validates and normalizes every request before
anything reaches Kafka, and enforces an SSRF (Server-Side Request Forgery) allowlist so
attacker-supplied image URLs can't reach internal hosts.

Both the data-path handling and the ingestion gate are backed by 100+ tests, including
Testcontainers integration suites; the CI badge above gates the backend and Flink unit tests only,
while the integration and frontend suites are run separately.

The eventtype pipeline's **DATA ingestion path** has been load-tested at **~10K req/s** over a multi-hour run (~250M+ requests, zero failures), with load generation, ingestion, and the processing pipeline as distinct roles across networked machines. Flink drained the resulting Kafka backlog into Postgres at **~12K events/s**.
See [Load testing](docs/TESTING.md#load-testing--ingestion-vs-drain) for the methodology.

### Out of scope

This is an exercise project focused on the data path's failure handling, not a production deployment. The following are deliberately not built:

- **Authentication / authorization** — the ingestion edges (`POST /api/events` + the React UI for eventtype, `POST /api/user-keys` for userKeys) are unauthenticated local testers, not hardened production boundaries: no login, API key, tenant isolation, or rate limiting, with the eventtype SSRF allowlist as the only request-level guard.
- **Secrets management & transport security** — credentials are supplied via a gitignored `.env`, but there is no Vault / Secrets Manager integration or rotation, and inter-service traffic on the local Docker network is plaintext (no TLS).
- **DLQ operations** — dead-letter records are captured and metered, but not consumed, replayed, or alerted on. See [DLQ operations](docs/eventtype/PIPELINE_FLOW.md#dlq-operations).
- **High availability** — no horizontal replication or cluster-level failover anywhere in the stack, so any single loss can mean data loss or downtime. See [Out of scope](docs/eventtype/PIPELINE_FLOW.md#out-of-scope).
- **Production paging** — basic backend/Flink-health email alerts exist for both pipelines (see [Observability](docs/eventtype/PIPELINE_FLOW.md#observability)), but not on-call escalation, Alertmanager-grade silencing/inhibition, or alerting on checkpoints or the DLQ.

## Overview

Events flow one direction — [Frontend → ] Backend → Kafka → Flink → sinks:

- [**Frontend**] (optional) — minimal event tester:
  - **eventtype** (http://localhost:3030) — React UI; submits `IMAGE` / `DATA` events.
  - **userKeys** — no UI.
- **Backend** (http://localhost:8030) — a narrow, synchronous
  [validation gate](backend/src/main/java/com/webcharm/backend/eventtype/api/EventController.java): every request
  passes through it before anything reaches Kafka, rejected with a 4xx if malformed or disallowed. One
  gate serves both pipelines:
  - **eventtype** (`POST /api/events`) — validates and publishes event JSON, and uploads file bytes to
    MinIO. `IMAGE`-by-URL events are checked against an SSRF allowlist at this edge (the trust boundary
    where user-supplied URLs enter), so a disallowed host (e.g. `http://169.254.169.254/`, the
    cloud-metadata endpoint) never reaches Kafka and Flink fetches without re-checking. With
    `PAYLOAD_ENCRYPTION_KEY` set, `DATA` payloads are AES-256-GCM encrypted into a self-describing
    envelope (algorithm, per-message nonce, ciphertext); unset is a pass-through.
  - **userKeys** (`POST /api/user-keys`) —
    [`UserKeyController`](backend/src/main/java/com/webcharm/backend/userkeys/api/UserKeyController.java)
    validates and publishes `{userId, key, value}` events.
- **Kafka** (localhost:9092) — durable event backbone; each pipeline owns its topics, with one
  pipeline active at a time:
  - **eventtype** — topics `events` (main) + `events-dlq` (dead-letter); both `IMAGE` and `DATA` ride the single `events` topic
    (one shared lifecycle; splitting by type would pay off only with per-type retention, scaling/ACLs,
    or ownership boundaries), keyed by the event `id` (UUID) so retries/duplicates of a logical event
    stay on one partition with per-event ordering preserved, leaving cross-event ordering free for
    parallelism.
  - **userKeys** — topics `user-keys` (main) + `user-keys-dlq` (dead-letter); keyed by `(userId, key)` to co-locate an aggregation
    key's events.
- **Flink** — the stream processor (UI at http://localhost:8081, port not exposed by default);
  everything after Kafka is its responsibility. It hosts two independent pipelines, one `StreamingJob` each, and
  exactly one runs at a time, selected with `./scripts/start.sh --pipeline <name>`:
  - **eventtype** (default) — consumes the `events` stream and routes by event type
    ([`StreamingJob`](flink/src/main/java/com/webcharm/pipeline/eventtype/StreamingJob.java) ·
    [pipeline flow](docs/eventtype/PIPELINE_FLOW.md)):
    - `IMAGE` events **always** land in MinIO (`images/{date}/{id}.{ext}`) — whether uploaded as a
      file (stored by the backend at ingestion) or supplied as a URL (fetched and **cloned** into
      MinIO by Flink; only the object key is persisted to Postgres, never the source URL).
      [Why clone rather than reference](docs/eventtype/PIPELINE_FLOW.md#async-image-enrichment): durable
      and self-contained.
    - `DATA` events are stored in Postgres — written in per-slot
      [committed batches](docs/eventtype/PIPELINE_FLOW.md#batched-postgres-writes) for throughput, flushed
      every checkpoint so the delivery guarantee is unchanged. An encrypted payload stays encrypted
      end-to-end: Flink passes the envelope through opaquely (it never needs the key) and persists it to
      the `payload` column, where a future consumer holding the key decrypts it with the same shared
      [`PayloadCipher`](contract-eventtype/src/main/java/com/webcharm/contract/eventtype/event/PayloadCipher.java).
  - **userKeys** — consumes `{userId, key, value}` events and sums `value` by `(userId, key)` over
    event-time tumbling windows into PostgreSQL, with exactly-once delivery via Flink's XA two-phase
    commit ([`StreamingJob`](flink/src/main/java/com/webcharm/pipeline/userkeys/StreamingJob.java) ·
    [pipeline flow](docs/userKeys/PIPELINE_FLOW.md)).

## Architecture

![Data Pipeline Architecture](docs/architecture.svg)

The pipeline services and their URLs are described in the [Overview](#overview); the supporting services:

| Service | Role | Access |
| --- | --- | --- |
| Kafka UI | inspect topics, consumer groups | http://localhost:8088 |
| MinIO | object storage; `minio-init` creates the `images` bucket at startup | Console: http://localhost:9011 (`minio` / `minio123`) |
| PostgreSQL | analytics store: eventtype `warehouse` db (processed events + aggregates), userKeys `userkeys` db (windowed sums) | localhost:5432 (`postgres` / `postgres`) |
| Grafana | per-pipeline analytics + Flink pipeline-health dashboards, plus email alert rules on backend/Flink health (both pipelines) | [http://localhost:3031](http://localhost:3031/dashboards) (`admin` / `admin`) |
| Prometheus | scrapes Flink metrics for operability | http://localhost:9090 |

## Getting Started

Requires Docker Desktop and Docker Compose v2. Run the Bash scripts from any Linux shell (WSL included):

```bash
find scripts -name '*.sh' -exec chmod +x {} +   # if not already executable
./scripts/start.sh
```

Then open the eventtype UI tester at http://localhost:3030 and send events.

### Choosing a pipeline

The stack runs one pipeline at a time; select it with `--pipeline` (default `eventtype`):

```bash
./scripts/start.sh --pipeline eventtype    # IMAGE / DATA events → MinIO + Postgres
./scripts/start.sh --pipeline userkeys     # {userId, key, value} → windowed sums → Postgres
```

Switching pipelines reuses the same images — no rebuild — but needs `--restart` so the prior
pipeline's job is cleared: `./scripts/start.sh --restart --pipeline userkeys`. After editing backend
or Flink source, add `--rebuild` to recompile the jar into the image, e.g:

```bash
./scripts/start.sh --pipeline userkeys --rebuild
```

Additional commands:

```bash
./scripts/start.sh --help                        # start options; after editing code/env, re-apply with --rebuild <service>
./scripts/docker-helper.sh --help                # build images (--build), stop the stack (--stop), or stream logs (--logs)
./scripts/test.sh --help                         # run tests; see docs/TESTING.md for suite details
./scripts/<pipeline>/send-event.sh --help        # send event(s)
```

## Testing

Unit, component, and integration test suites and how to run them are documented in [TESTING.md](docs/TESTING.md).

## Analytics

Post-hoc vs. Flink-pre-aggregated queries, the windowing behavior, and how to run them (CLI and Grafana), documented per pipeline:

- **eventtype** — [ANALYTICS.md](docs/eventtype/ANALYTICS.md).
- **userKeys** — windowed sums, in the [pipeline flow](docs/userKeys/PIPELINE_FLOW.md).

## License

Released under the MIT License. See [LICENSE](LICENSE).
