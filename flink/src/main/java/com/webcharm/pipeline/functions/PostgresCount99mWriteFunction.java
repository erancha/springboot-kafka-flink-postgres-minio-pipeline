package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.sinks.PostgresEventTypeCount99mWriter;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.EventTypeCount99m;

/**
 * Writes EventTypeCount99m records to Postgres and routes permanent JDBC failures to a DLQ.
 * Mirrors PostgresWriteFunction's fault-handling strategy for the windowed-counts path.
 */
public class PostgresCount99mWriteFunction extends AbstractPostgresWriteFunction<EventTypeCount99m> {

  public PostgresCount99mWriteFunction(DlqStage stage) {
    super(stage);
  }

  @Override
  protected JdbcWriter<EventTypeCount99m> createWriter() {
    return new PostgresEventTypeCount99mWriter();
  }

  @Override
  protected String logContextString(EventTypeCount99m eventTypeCount) {
    return "count type=" + eventTypeCount.getEventType() + " start=" + eventTypeCount.getWindowStart();
  }

}
