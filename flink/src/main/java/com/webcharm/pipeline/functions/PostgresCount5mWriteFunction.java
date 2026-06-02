package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.sinks.PostgresEventTypeCount5mWriter;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.EventTypeCount5m;

/**
 * Writes EventTypeCount5m records to Postgres and routes permanent JDBC failures to a DLQ.
 * Mirrors PostgresWriteFunction's fault-handling strategy for the windowed-counts path.
 */
public class PostgresCount5mWriteFunction extends AbstractPostgresWriteFunction<EventTypeCount5m> {

  public PostgresCount5mWriteFunction(DlqStage stage) {
    super(stage);
  }

  @Override
  protected JdbcWriter<EventTypeCount5m> createWriter() {
    return new PostgresEventTypeCount5mWriter();
  }

  @Override
  protected String logContextString(EventTypeCount5m eventTypeCount) {
    return "count type=" + eventTypeCount.getEventType() + " start=" + eventTypeCount.getWindowStart();
  }

}
