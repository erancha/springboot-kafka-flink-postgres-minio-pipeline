package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.sinks.PostgresProcessedEventWriter;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.ProcessedEvent;

/**
 * Writes ProcessedEvents to Postgres and routes permanent JDBC failures to a DLQ.
 * Permanent failures cannot be fixed by checkpoint replay — routing to DLQ prevents infinite restart loops.
 */
public class PostgresWriteFunction extends AbstractPostgresWriteFunction<ProcessedEvent> {

  public PostgresWriteFunction(DlqStage stage) {
    super(stage);
  }

  @Override
  protected JdbcWriter<ProcessedEvent> createWriter() {
    return new PostgresProcessedEventWriter();
  }

  @Override
  protected String logContextString(ProcessedEvent event) {
    return "event id=" + event.getId();
  }

}
