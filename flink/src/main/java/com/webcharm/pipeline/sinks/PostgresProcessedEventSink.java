package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.ProcessedEvent;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * Writes processed events (both DATA and IMAGE) to the processed_events table.
 * Implements Sink<T> rather than the deprecated RichSinkFunction so the sink participates
 * in Flink's checkpoint barriers. Delivery semantic is at-least-once: on recovery Flink
 * replays every Kafka offset since the last completed checkpoint, and the writer's idempotent
 * upsert (ON CONFLICT (id) DO UPDATE) absorbs duplicate writes safely.
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
