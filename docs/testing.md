# Testing

[Layers](#test-layers) · [Running](#running) · [Load testing](#load-testing--ingestion-vs-drain) ·
[Alerting](#alerting-end-to-end-check) · pipeline specifics:
[eventtype](#eventtype--load-test-scope) · [userKeys](#userkeys--exactly-once-under-test)

## Test layers

Tests are organized into layers by how much each one has to spin up:

- **Unit** — a single class or function in isolation, all dependencies mocked; no framework, no Docker.
- **Component** — one architectural layer (e.g. the HTTP/API layer, or the UI) with its framework or browser context loaded, but the surrounding layers and external systems (broker, object store, database) mocked.
- **Integration** — vertical slices against real infrastructure started on demand via Testcontainers and torn down afterward; needs Docker.
- **Load** — sustained-traffic runs measuring ingestion throughput, latency, and drain rate (see [Load testing](#load-testing--ingestion-vs-drain)).
- **Alerting** — an end-to-end check that the alert path fires on a real outage and clears on recovery.

## Running

`./scripts/test.sh` is the unified runner (`--help` lists the suites); it builds from source, selects a JDK 21, and installs frontend dependencies on demand.

```bash
./scripts/test.sh                    # every suite
./scripts/test.sh backend frontend   # only the named suites
```

Integration suites require Docker (Testcontainers pulls images on first run); every other suite needs neither Docker nor a running stack.

## Load testing — ingestion vs. drain

One harness serves both pipelines; `jmeter-helper.sh --pipeline <name>` selects the plan. Split the
work across three roles, ideally three separate machines:

- **A — Data pipeline:** the Docker stack (Kafka → Flink → Postgres/MinIO).
- **B — Backend:** the Spring Boot ingestion service plus its own Prometheus + Grafana, pointed at A's broker across the network.
- **C — JMeter:** the load generator, flooding B with HTTP events.

C should stay off B: the load generator competes with the backend for CPU, so co-locating them caps
ingestion — read that ceiling as load-generator-bound, not a true backend limit.

```bash
# on B — backend (+ Prometheus + Grafana) against A's broker:
./scripts/start-backend.sh --kafka-host <A-ip>

# on C — drive the load at B across the network (omit --backend-host when C runs on B):
./scripts/jmeter-helper.sh --pipeline <name> --backend-host <B-ip> -t 500 -i 20000
```

`jmeter-helper.sh` marks a request done the instant B publishes it and Kafka acks — nothing
downstream. Its throughput and latencies therefore describe B's acceptance edge alone; whether A
keeps up with that inflow is the separate question the drain rate answers.

**Where to read the numbers** — these are the live sources of truth:

- Ingestion throughput / latency / errors → JMeter's run report on C, under `jmeter/results/`.
- Backend CPU & heap → the *Backend Ingestion* Grafana dashboard on B.
- Drain rate → the active pipeline's dashboard on A's Grafana (the rate of writes into Postgres).

## Alerting end-to-end check

```bash
./scripts/alert-test.sh
```

Runs against the live stack from `start.sh`: it induces a real outage, polls until the alert rule reaches `firing`, then restores the service and waits for it to clear — verifying the metrics-reporter → Prometheus → Grafana path. Email delivery itself is not asserted. Takes the stack down briefly, and with SMTP configured in `.env` it dispatches a real alert email.

## Pipeline-specific considerations

### eventtype — load-test scope

The JMeter plan (`jmeter/eventtype/backend-load.jmx`), selected with `--pipeline eventtype`, drives
only `DATA` events, so the headline ingestion and drain figures cover the **DATA → Postgres** path.
The IMAGE path — URL fetch, MinIO upload, async enrichment — is exercised by the integration suites
but **not** load-tested, so its throughput under sustained traffic is uncharacterized.

### userKeys — exactly-once under test

`UserKeyAggregationIT` runs the windowed-sum topology and its XA exactly-once sink against a
Testcontainers Postgres started with `max_prepared_transactions` enabled. The sink does a plain `INSERT` keyed by
`(window_start, user_id, key)` — no upsert — so a double commit would violate the primary key and fail
the job; a clean run landing exactly the expected rows is the suite's evidence that each window
committed once. The delivery mechanics (XA two-phase commit, `transactionPerConnection`,
`max_prepared_transactions` sizing) are described in the
[userKeys pipeline flow](userkeys/pipeline-flow.md#exactly-once-delivery-into-postgres).
