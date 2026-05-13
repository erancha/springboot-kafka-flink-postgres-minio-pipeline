# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

A real-time data pipeline exercise demonstrating end-to-end event streaming. Users submit events through a React UI; events flow through Spring Boot → Kafka → Flink → MinIO (images) or PostgreSQL (data).

## Common Commands

All stack operations go through scripts in `scripts/`:

```bash
./scripts/up.sh          # Start full Docker Compose stack
./scripts/down.sh        # Stop (keeps volumes)
./scripts/restart.sh     # Stop, rebuild, and restart
./scripts/clean.sh       # Stop and remove volumes; add --prune to purge dangling images
./scripts/build.sh       # Build all Docker images
./scripts/ps.sh          # Show running containers and service URLs
./scripts/health.sh      # Check container health
./scripts/logs.sh [service]          # Stream logs; -e/--errors filters to errors only
./scripts/sql.sh                     # Interactive psql session
./scripts/sql-file.sh <path>         # Execute SQL file against Postgres
./scripts/minio-ls.sh [prefix]       # List MinIO bucket objects
./scripts/minio-cat.sh <key>         # Print MinIO object contents
./scripts/test.sh [suite...]         # Run tests: backend flink flink-it frontend (default: all)
```

Building components individually (not normally needed — Docker handles it):

```bash
# Backend (Java 21, Maven)
cd backend && mvn -q -DskipTests package

# Flink job (Java 21, Maven, fat JAR via maven-shade)
cd flink && mvn -q -DskipTests package

# Frontend (Node 20)
cd frontend && npm install && npm run build
```

## Architecture & Data Flow

```
React UI (port 3030)
    │  POST /api/events
    ▼
Spring Boot (port 8030)
    │  validates, publishes JSON to Kafka topic "events"
    ▼
Kafka (KRaft, port 9092)  ◄──── Kafka UI (port 8088)
    │
    ▼
Flink StreamingJob
    ├── eventType == "IMAGE"  →  MinIO bucket "images" (port 9000/9001)
    │                             key: images/{YYYY-MM-DD}/{uuid}.{ext}
    └── eventType == "DATA"   →  PostgreSQL "warehouse" db (port 5432)
                                  table: processed_events
```

The Flink job is submitted by the `flink-job` Docker service at startup and runs on the `flink-jobmanager` / `flink-taskmanager` pair. Flink UI is at port 8081.

## Event Schema

**DATA event** (`samples/data_event.json`):

```json
{ "eventType": "DATA", "payload": { ... } }
```

Stored in `processed_events` table with columns: `id` (UUID), `event_type`, `event_time`, `source`, `payload` (JSONB), `inserted_at`.

**IMAGE event** (`samples/image_event_url.json`):

```json
{ "eventType": "IMAGE", "imageUrl": "https://..." }
```

Or with `imageBase64` + `imageContentType` fields. Flink fetches/decodes and stores in MinIO.

## Key Design Points

- **Kafka topic `events`**: single topic, event id used as partition key (ordering per-event, parallelism across events)
- **Backend validation split**: HTTP-level validation (Bean Validation on `EventRequest`) is separate from business validation in `EventController`
- **Flink routing**: `StreamingJob.java` fans out based on `eventType`: IMAGE events go through `MinioUploadFunction` (map) then `PostgresProcessedEventSink`; DATA events go directly to `PostgresProcessedEventSink`
- **Nginx** (`frontend/nginx.conf`): proxies `/api/` to `backend:8030` in production; Vite dev server (`vite.config.ts`) proxies to `localhost:8030` during local development
- **No ZooKeeper**: Kafka runs in KRaft mode

## Service Credentials (local only)

| Service    | Connection / Credentials                                        |
| ---------- | --------------------------------------------------------------- |
| PostgreSQL | `localhost:5432` db=`warehouse` user=`postgres` pass=`postgres` |
| MinIO      | `localhost:9000` user=`minio` pass=`minio123` bucket=`images`   |
| Kafka      | `localhost:9092` topic=`events`                                 |

## Java Version Notes

- Backend: Java **21** (Spring Boot 3.5.14)
- Flink job: Java **21** (Flink 2.2.1)
- Both built with Maven; fat JAR for Flink via `maven-shade-plugin`

## Testing

```bash
mvn -f backend/pom.xml test          # 7 backend tests (no Docker)
mvn -f flink/pom.xml test            # 12 Flink unit tests (no Docker)
mvn -f flink/pom.xml verify          # 12 unit + 3 Testcontainers IT (requires Docker)
cd frontend && npm test              # 5 frontend tests (no Docker)
```

**Flink-specific gotchas:**
- Flink 2.2.1 changed `RichMapFunction.open()` signature to `open(OpenContext)` — do not use `open(Configuration)`.
- `maven-surefire-plugin` runs `*Test.java` (`mvn test`); `maven-failsafe-plugin` runs `*IT.java` (`mvn verify`).
- Testcontainers on WSL2/Docker Desktop requires `flink/src/test/resources/docker-java.properties` with `api.version=1.41` (already present).
