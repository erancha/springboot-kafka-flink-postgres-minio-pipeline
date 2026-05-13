-- Analytics SQL examples
--
-- All queries run against PostgreSQL. Some aggregate at query time (post-hoc),
-- others query a table pre-aggregated by Flink in real time.
--
-- Run with:
--   ./scripts/sql-file.sh samples/analytics.sql

-- Post-hoc: Count events by type (computed at query time)
SELECT event_type, COUNT(*)
FROM processed_events
GROUP BY event_type
ORDER BY COUNT(*) DESC;

-- Post-hoc: Retrieve latest processed records (computed at query time)
SELECT id, event_type, event_time, source, image_object_key, inserted_at
FROM processed_events
ORDER BY inserted_at DESC
LIMIT 20;

-- Post-hoc: Aggregate events by hour (computed at query time)
SELECT date_trunc('hour', event_time) AS hour, event_type, COUNT(*)
FROM processed_events
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- Real-time: 5-minute event count per type (pre-aggregated by Flink, stored in event_type_counts_5m)
-- This table is populated in real time by Flink's 5-minute tumbling window job
SELECT window_start, window_end, event_type, event_count
FROM event_type_counts_5m
ORDER BY window_start DESC, event_type;
