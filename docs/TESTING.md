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

## Alerting end-to-end check — requires the running stack

```bash
./scripts/alert-test.sh
```

Unlike the hermetic suites above, this runs against the live stack started by `start.sh`. It stops the Flink TaskManager to induce a real outage, polls Grafana's rule-state API until the `Flink target down` rule reaches `firing`, then restarts the TaskManager and waits for it to clear. Verifies the alert path — Flink reporter → Prometheus → Grafana rule evaluation — while email delivery itself is not asserted.

> Takes the pipeline down for ~2 minutes, and if SMTP is configured in `.env`, firing dispatches a real alert email.
