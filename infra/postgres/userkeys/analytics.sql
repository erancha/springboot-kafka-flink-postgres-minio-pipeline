-- Analytics SQL examples for the userKeys pipeline.
--
-- The userKeys tables live in their own `userkeys` database (see init.sql), so this file connects
-- there first. user_key_aggregates is pre-aggregated by Flink in real time: one row per
-- (window, userId, key) holding the value summed over that tumbling window.
--
-- Run with:
--   ./scripts/sql-helper.sh -f infra/postgres/userkeys/analytics.sql

\connect userkeys

-- \echo '== [user_key_aggregates] Last n window results =='
-- SELECT window_start, window_end, user_id, key, value_sum
-- FROM user_key_aggregates
-- ORDER BY window_start DESC
-- LIMIT 10;

\echo '== [user_key_aggregates] Count of aggregated records (one row per closed window per (userId, key)) =='
-- Total window-result rows written, mirroring the "Cumulative aggregate rows written" dashboard
-- panel. This counts rows, not events: each row is one (userId, key) tumbling window, so it grows by
-- the number of active (userId, key) pairs each time a window fires, not by the inbound event rate.
SELECT COUNT(*) AS aggregated_records
FROM user_key_aggregates;

\echo '== [user_key_aggregates] Real-time: latest windowed (userId, key) sums (pre-aggregated by Flink) =='
SELECT window_start, window_end, user_id, key, value_sum
FROM user_key_aggregates
ORDER BY window_start DESC, user_id, key
LIMIT 50;

\echo '== [user_key_aggregates] Lifetime sum per (userId, key) across all windows =='
-- Summing the windowed value_sum collapses the per-window rows into a running lifetime total per
-- (userId, key), modulo any window still open (event-time watermark not yet past its end).
SELECT user_id, key, SUM(value_sum) AS total_value
FROM user_key_aggregates
GROUP BY user_id, key
ORDER BY total_value DESC
LIMIT 10;

\echo '== [user_key_aggregates] Lifetime sum per user, with grand total (NULL user_id = grand total) =='
SELECT user_id, SUM(value_sum) AS total_value
FROM user_key_aggregates
GROUP BY ROLLUP(user_id)
ORDER BY user_id NULLS LAST
LIMIT 10;

\echo '== [user_key_aggregates] Top 10 keys by lifetime value across all users =='
SELECT key, SUM(value_sum) AS total_value
FROM user_key_aggregates
GROUP BY key
ORDER BY total_value DESC
LIMIT 10;
