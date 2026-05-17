package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.sinks.PostgresEventTypeCount5mWriter;
import com.webcharm.pipeline.types.EventTypeCount5m;

/**
 * Writes EventTypeCount5m records to Postgres and routes permanent JDBC failures to a DLQ.
 * Mirrors PostgresWriteFunction's fault-handling strategy for the windowed-counts path.
 */
public class PostgresCount5mWriteFunction extends AbstractPostgresWriteFunction<EventTypeCount5m> {

  /** Creates a writer that upserts into the event_type_counts_5m table. */
  @Override
  protected JdbcWriter<EventTypeCount5m> createWriter() {
    return new PostgresEventTypeCount5mWriter();
  }

  /** Returns a log-friendly string identifying record, e.g. "count type=<type> start=<window-start>" for use in DLQ log messages. */
  @Override
  protected String logContextString(EventTypeCount5m eventTypeCount) {
    return "count type=" + eventTypeCount.getEventType() + " start=" + eventTypeCount.getWindowStart();
  }

}
