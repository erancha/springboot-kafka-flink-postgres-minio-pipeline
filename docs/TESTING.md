# Testing

Tests are organized into layers by how much each one has to spin up:

- **Unit** — a single class or function in isolation, all dependencies mocked; no framework, no Docker.
- **Component** — one architectural layer (e.g. the HTTP/API layer, or the UI) with its framework or browser context loaded, but the surrounding layers and external systems (broker, object store, database) mocked.
- **Integration** — vertical slices against real infrastructure started on demand via Testcontainers and torn down afterward; needs Docker.
- **Load** — sustained-traffic runs measuring ingestion throughput, latency, and drain rate.
- **Alerting** — an end-to-end check that the alert path fires on a real outage and clears on recovery.

What each layer actually exercises is documented per pipeline:

- [Event pipeline](eventtype/TESTING.md)

## Running

`./scripts/test.sh` is the unified runner (`--help` lists the suites); it builds from source, selects a JDK 21, and installs frontend dependencies on demand.

```bash
./scripts/test.sh                    # every suite
./scripts/test.sh backend frontend   # only the named suites
```

Integration suites require Docker (Testcontainers pulls images on first run); every other suite needs neither Docker nor a running stack.

## Alerting end-to-end check

```bash
./scripts/alert-test.sh
```

Runs against the live stack from `start.sh`: it induces a real outage, polls until the alert rule reaches `firing`, then restores the service and waits for it to clear — verifying the metrics-reporter → Prometheus → Grafana path. Email delivery itself is not asserted. Takes the stack down briefly, and with SMTP configured in `.env` it dispatches a real alert email.
