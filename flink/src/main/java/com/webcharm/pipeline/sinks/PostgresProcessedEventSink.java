package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.ProcessedEvent;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * Writes processed events (both DATA and IMAGE) to the processed_events table.
 * Implements Sink rather than the deprecated RichSinkFunction so the sink participates
 * in Flink's checkpoint barriers. Delivery is effective exactly-once: write
 * failures propagate as IOException so Flink replays from the last checkpoint, and the
 * idempotent upsert on id (ON CONFLICT (id) DO UPDATE) absorbs any duplicate replays.
 * Not 2PC. See flink/PIPELINE_FLOW.md for the full failure inventory.
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
