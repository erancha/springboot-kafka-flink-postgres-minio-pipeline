# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

A real-time data pipeline exercise demonstrating end-to-end event streaming. Users submit events through a React UI; events flow through Spring Boot → Kafka → Flink → MinIO (images) or PostgreSQL (data).

## Common Commands

All stack operations go through scripts in `scripts/`:

```bash
./scripts/start.sh           # Start full Docker Compose stack; --restart stops first, --rebuild rebuilds images
./scripts/stop.sh            # Stop (keeps volumes); --remove-volumes removes volumes; --prune-dangling-images also purges build artifacts
./scripts/build.sh           # Build all Docker images
./scripts/ps.sh              # Show running containers and service URLs
./scripts/health.sh          # Check container health
./scripts/logs.sh [service]  # Stream logs; -e/--errors filters to errors only
./scripts/sql.sh             # Interactive psql session
./scripts/sql-file.sh <path> # Execute SQL file against Postgres
./scripts/minio-ls.sh [prefix] # List MinIO bucket objects
./scripts/minio-cat.sh <key>   # Print MinIO object contents
./scripts/test.sh [suite...]   # Run tests: backend flink flink-it frontend (default: all)
```

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
- **Flink routing**: `StreamingJob.java` fans out based on `eventType`: IMAGE events go through `MinioUploadFunction` (ProcessFunction) — keyed events pass through, URL events are fetched and uploaded to MinIO; DATA events go directly to `PostgresProcessedEventSink`
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
