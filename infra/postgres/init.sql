CREATE TABLE IF NOT EXISTS processed_events (
  id UUID PRIMARY KEY,
  event_type TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  payload JSONB NULL,
  image_object_key TEXT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_type_time
  ON processed_events (event_type, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_processed_events_inserted_at
  ON processed_events (inserted_at DESC);

CREATE TABLE IF NOT EXISTS event_type_counts_5m (
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,
  event_count BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (window_start, event_type)
);

CREATE INDEX IF NOT EXISTS idx_event_type_counts_5m_window_start
  ON event_type_counts_5m (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_event_type_counts_5m_event_type
  ON event_type_counts_5m (event_type);
