# Analytics Queries

All analytics queries are defined in [infra/postgres/eventtype/analytics.sql](../../infra/postgres/eventtype/analytics.sql) and query PostgreSQL; the tables they read are created in [infra/postgres/eventtype/init.sql](../../infra/postgres/eventtype/init.sql). They differ in _when_ they are computed:

**Post-hoc analytics** (computed at query time):

- Count events by type
- Retrieve latest records
- Aggregate by hour

These scan the `processed_events` table and run standard SQL aggregations. Flink is not involved.

**Real-time analytics** (pre-aggregated by Flink):

Flink computes continuously; queries read pre-computed results (no query-time latency):

- 5-minute tumbling-window event count per `eventType` (stored in `event_type_counts_agg`)
- 10-minute tumbling-window count of stored images per size bucket (stored in `image_size_buckets_agg`)

**Flink windowing behavior, by example:**

Take the per-type count ([`StreamingJob.buildEventTypeCounts`](../../flink/src/main/java/com/webcharm/pipeline/eventtype/StreamingJob.java)): it groups events by `eventType` within each 5-minute window and counts them, producing one row per `(eventType, window_start)`. Flink writes a row only for a type that received at least one event in that window — there are no zero rows. So a window holds a row for every type that saw traffic and none for a type that saw none: the same window can have a `DATA` row and no `IMAGE` row, or the reverse. Emitting zero-count rows would require explicit late-firing or allowed-lateness policies.

## Running the queries

Via CLI:

```bash
./scripts/sql-helper.sh -f infra/postgres/eventtype/analytics.sql
```

Via Grafana: open the
[eventtype Stored Aggregates Analytics dashboard](http://localhost:3031/d/processed-events/eventtype-stored-aggregates-analytics?orgId=1&refresh=10s)
(user: `admin`, pass: `admin`).

The CLI runs every query in `analytics.sql`; the Grafana dashboard renders only the Flink
pre-aggregated tables (`event_type_counts_agg`, `image_size_buckets_agg`) as live panels.

![Grafana dashboard screenshot](../Grafana.jpg)
