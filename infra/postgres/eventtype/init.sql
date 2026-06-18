CREATE TABLE IF NOT EXISTS processed_events (
  id UUID PRIMARY KEY,
  event_type TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  payload JSONB NULL,
  image_object_key TEXT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_type_counts_agg (
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,
  event_count BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (window_start, event_type)
);

CREATE INDEX IF NOT EXISTS idx_event_type_counts_agg_window_start
  ON event_type_counts_agg (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_event_type_counts_agg_event_type
  ON event_type_counts_agg (event_type);

CREATE TABLE IF NOT EXISTS image_size_buckets_agg (
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  bucket TEXT NOT NULL,
  image_count BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (window_start, bucket)
);

CREATE INDEX IF NOT EXISTS idx_image_size_buckets_agg_window_start
  ON image_size_buckets_agg (window_start DESC);
