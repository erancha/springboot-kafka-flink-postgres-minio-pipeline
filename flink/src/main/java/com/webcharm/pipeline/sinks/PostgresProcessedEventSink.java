package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.ProcessedEvent;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * Writes processed events (both DATA and IMAGE) to the processed_events table.
 * Implements Sink rather than the deprecated RichSinkFunction so the sink participates
 * in Flink's checkpoint barriers. Target delivery is exactly-once observable, achieved
 * by at-least-once replay combined with an idempotent upsert on id (ON CONFLICT (id) DO UPDATE).
 * Not 2PC.
 *
 * Current gap: PostgresProcessedEventWriter swallows write exceptions instead of re-throwing,
 * so a failed write is silently dropped (at-most-once on failure, not exactly-once).
 * See flink/PIPELINE_FLOW.md for the full failure inventory.
 */
public class PostgresProcessedEventSink implements Sink<ProcessedEvent> {

  /**
   * Creates the writer that upserts each ProcessedEvent into the processed_events table.
   * Called once per parallel task slot by the Flink runtime.
   */
  @Override
  public SinkWriter<ProcessedEvent> createWriter(WriterInitContext context) throws IOException {
    return new PostgresProcessedEventWriter();
  }
}
