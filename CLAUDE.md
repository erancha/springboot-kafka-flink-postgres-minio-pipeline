# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

A real-time data pipeline exercise demonstrating end-to-end event streaming. Users submit events through a React UI; events flow through Spring Boot → Kafka → Flink → MinIO (images) or PostgreSQL (data).

## Common Commands

All stack operations go through scripts in `scripts/`. Scripts that take options print
their own usage via `-h`/`--help` — consult that rather than relying on flags listed here.

- `start.sh` — start the stack, or rebuild/recreate only named services
- `docker-helper.sh` — build images (`--build`), stop the stack (`--stop`), or stream logs (`--logs`)
- `sql-helper.sh` — interactive psql, inline SQL (`-c`), or a host SQL file (`-f`) against Postgres
- `minio-helper.sh` — list (`ls`) / print (`cat`) MinIO bucket objects
- `test.sh` — run the test suites
- `send-event.sh` — send one DATA and/or IMAGE event and show where it landed

## Architecture & Data Flow

```
React UI (port 3030)
    │  POST /api/events
    ▼
Spring Boot (port 8030)
    │  file upload: uploads bytes to MinIO, publishes pointer key to Kafka
    │  url / data:  publishes event JSON to Kafka
    ▼
Kafka (KRaft, port 9092)  ◄──── Kafka UI (port 8088)
    │
    ▼
Flink StreamingJob
    ├── IMAGE (imageObjectKey)  →  PostgreSQL (MinIO upload already done by backend)
    ├── IMAGE (imageUrl)        →  fetch URL → MinIO "images" (port 9000/9001) → PostgreSQL
    └── DATA                   →  PostgreSQL "warehouse" db (port 5432)
                                   table: processed_events
```

The Flink job is submitted by the `flink-job` Docker service at startup and runs on the `flink-jobmanager` / `flink-taskmanager` pair. Flink UI is at port 8081.

## Event Schema

**DATA event** — arbitrary payload; Flink writes it to Postgres `processed_events`.

**IMAGE event** — two sub-paths: file-upload (backend uploads to MinIO, publishes a pointer key, Flink passthrough) and URL (Flink fetches the URL, uploads to MinIO, then writes to Postgres).

## Key Design Points

- **Kafka topic `events`**: single topic, event id used as partition key (ordering per-event, parallelism across events)
- **Backend validation split**: HTTP-level validation (Bean Validation on `EventRequest`) is separate from business validation in `EventController`
- **Flink topology**: the fan-out by `eventType`, async MinIO enrichment, tumbling-window aggregations, and single dead-letter path are defined and documented in `flink/src/main/java/com/webcharm/pipeline/StreamingJob.java` (class Javadoc + inline comments)
- **Nginx** (`frontend/nginx.conf`): proxies `/api/` to `backend:8030` in production; Vite dev server (`vite.config.ts`) proxies to `localhost:8030` during local development
- **No ZooKeeper**: Kafka runs in KRaft mode

## Service Credentials (local only)

| Service    | Connection / Credentials                                        |
| ---------- | --------------------------------------------------------------- |
| PostgreSQL | `localhost:5432` db=`warehouse` user=`postgres` pass=`postgres` |
| MinIO      | `localhost:9000` user=`minio` pass=`minio123` bucket=`images`   |
| Kafka      | `localhost:9092` topic=`events`                                 |

## Java / Build Notes

- Backend and Flink job both require Java 21; Flink fat JAR built via `maven-shade-plugin`
- Flink changed `RichMapFunction.open()` signature to `open(OpenContext)` — do not use `open(Configuration)`
- `maven-surefire-plugin` runs `*Test.java` (`mvn test`); `maven-failsafe-plugin` runs `*IT.java` (`mvn verify`)
- Testcontainers on WSL2/Docker Desktop requires `flink/src/test/resources/docker-java.properties` with `api.version=1.41` (already present)
