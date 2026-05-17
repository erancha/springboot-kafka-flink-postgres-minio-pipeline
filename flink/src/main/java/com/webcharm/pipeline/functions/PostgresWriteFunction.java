package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.sinks.PostgresProcessedEventWriter;
import com.webcharm.pipeline.types.ProcessedEvent;

/**
 * Writes ProcessedEvents to Postgres and routes permanent JDBC failures to a DLQ.
 * Permanent failures cannot be fixed by checkpoint replay — routing to DLQ prevents infinite restart loops.
 */
public class PostgresWriteFunction extends AbstractPostgresWriteFunction<ProcessedEvent> {

  /** Creates a writer that upserts into the processed_events table. */
  @Override
  protected JdbcWriter<ProcessedEvent> createWriter() {
    return new PostgresProcessedEventWriter();
  }

  /** Returns a log-friendly string identifying record, e.g. "event id=<uuid>" for use in DLQ log messages. */
  @Override
  protected String logContextString(ProcessedEvent event) {
    return "event id=" + event.getId();
  }

}
