-- Analytics SQL examples
--
-- All queries run against PostgreSQL. Some aggregate at query time (post-hoc),
-- others query a table pre-aggregated by Flink in real time.
--
-- Run with:
--   ./scripts/sql-file.sh infra/postgres/analytics.sql

\echo '== [processed_events] Post-hoc: Count events by type (computed at query time) =='
SELECT event_type, COUNT(*)
FROM processed_events
GROUP BY event_type
ORDER BY COUNT(*) DESC;

\echo '== [processed_events] Post-hoc: Retrieve latest processed records (computed at query time) =='
SELECT id, event_type, event_time, source, image_object_key, inserted_at
FROM processed_events
ORDER BY inserted_at DESC
LIMIT 20;

\echo '== [processed_events] Post-hoc: Aggregate events by hour (computed at query time) =='
SELECT date_trunc('hour', event_time) AS hour, event_type, COUNT(*)
FROM processed_events
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

\echo '== [event_type_counts_agg] Real-time: 5-minute event count per type (pre-aggregated by Flink) =='
SELECT window_start, window_end, event_type, event_count
FROM event_type_counts_agg
ORDER BY window_start DESC, event_type;

\echo '== [image_size_buckets_agg] Real-time: 10-minute stored-image count per size bucket (pre-aggregated by Flink) =='
SELECT window_start, window_end, bucket, image_count
FROM image_size_buckets_agg
ORDER BY window_start DESC, bucket;
