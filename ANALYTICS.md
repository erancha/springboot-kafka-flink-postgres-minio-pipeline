# Analytics Queries

All analytics queries are defined in [infra/postgres/analytics.sql](infra/postgres/analytics.sql) and query PostgreSQL. They differ in _when_ they are computed:

**Post-hoc analytics** (computed at query time):

- Count events by type
- Retrieve latest records
- Aggregate by hour

These scan the `processed_events` table and run standard SQL aggregations. Flink is not involved.

**Real-time analytics** (pre-aggregated by Flink):

Flink computes continuously; queries read pre-computed results (no query-time latency):

- 5-minute tumbling-window event count per `eventType` (stored in `event_type_counts_99m`)
- 10-minute tumbling-window count of stored images per size bucket (stored in `image_size_buckets_99m`)

**Flink windowing behavior:**

The 5-minute tumbling-window aggregation ([`StreamingJob.buildWindowedCounts`](flink/src/main/java/com/webcharm/pipeline/StreamingJob.java)) uses Flink's default behavior: it only emits window results for windows that contain at least one event. Empty windows are not materialized. This means the `event_type_counts_99m` table will only have rows for time periods when events actually arrived. If there are no `DATA` events in a 5-minute window, that window will not appear in the results, even if the same period had `IMAGE` events. This is standard Flink behavior and conserves storage; to include all windows (including empty ones), the job would need explicit late-firing or allowed lateness policies.

## Running the queries

Via CLI:

```bash
./scripts/sql-file.sh infra/postgres/analytics.sql
```

Via Grafana dashboard:
Access [http://localhost:3031](http://localhost:3031/d/processed-events/processed-events-analytics?orgId=1&refresh=10s) (user: `admin`, pass: `admin`), then:

- Dashboards → Browse → **Processed Events Analytics**

Both CLI and Grafana execute the same SQL against PostgreSQL; the difference is presentation (one-off results vs. live dashboard).

![Grafana dashboard screenshot](docs/Grafana.jpg)
