package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.EventTypeCount5m;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * Writes per-window event-type counts to the event_type_counts_5m table.
 * Implements the Sink API instead of the deprecated RichSinkFunction so the sink participates
 * in Flink's checkpoint barriers. Exactly-once observable result is achieved by at-least-once
 * replay (writer re-throws on failure, job restarts from last checkpoint) combined with an
 * idempotent upsert on (window_start, event_type); the row is overwritten with the same value
 * on replay. Not 2PC.
 */
public class PostgresEventTypeCount5mSink implements Sink<EventTypeCount5m> {

  @Override
  public SinkWriter<EventTypeCount5m> createWriter(WriterInitContext context) throws IOException {
    return new PostgresEventTypeCount5mWriter();
  }
}
