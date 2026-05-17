package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.sinks.PostgresEventTypeCount5mWriter;
import com.webcharm.pipeline.types.EventTypeCount5m;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * Writes EventTypeCount5m records to Postgres and routes permanent JDBC failures to a DLQ.
 * Mirrors PostgresWriteFunction's fault-handling strategy for the windowed-counts path.
 */
public class PostgresCountWriteFunction extends AbstractPostgresWriteFunction<EventTypeCount5m> {

  /** Creates a writer that upserts into the event_type_counts_5m table. */
  @Override
  protected SinkWriter<EventTypeCount5m> createWriter() {
    return new PostgresEventTypeCount5mWriter();
  }

  /** Returns a log-friendly string identifying record, e.g. "count type=<type> start=<window-start>" for use in DLQ log messages. */
  @Override
  protected String logContextString(EventTypeCount5m eventTypeCount) {
    return "count type=" + eventTypeCount.getEventType() + " start=" + eventTypeCount.getWindowStart();
  }

}
