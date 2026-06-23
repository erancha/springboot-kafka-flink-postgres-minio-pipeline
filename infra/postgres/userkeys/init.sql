-- Runs once at first Postgres init, against the default database, then switches into the userKeys
-- database it creates. The userKeys pipeline owns this database and every table in it.
CREATE DATABASE userkeys;

\connect userkeys

-- One row per (window, userId, key): the value summed over that tumbling window. The Flink job
-- writes each window result once through an exactly-once XA sink, so a plain INSERT suffices and the
-- primary key never collides. window_start leads the key so inserts append in event-time order.
CREATE TABLE IF NOT EXISTS user_key_aggregates (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  user_id      TEXT        NOT NULL,
  key          TEXT        NOT NULL,
  value_sum    BIGINT      NOT NULL,
  PRIMARY KEY (window_start, user_id, key)
);

CREATE INDEX IF NOT EXISTS idx_user_key_aggregates_user_key
  ON user_key_aggregates (user_id, key);
