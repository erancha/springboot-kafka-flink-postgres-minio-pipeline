package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.sinks.PostgresEventTypeCountAggWriter;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.EventTypeCountAgg;

/**
 * Writes EventTypeCountAgg records to Postgres and routes permanent JDBC failures to a DLQ.
 * Mirrors PostgresWriteFunction's fault-handling strategy for the windowed-counts path.
 */
public class PostgresCountAggWriteFunction extends AbstractPostgresWriteFunction<EventTypeCountAgg> {

  public PostgresCountAggWriteFunction(DlqStage stage) {
    super(stage);
  }

  @Override
  protected JdbcWriter<EventTypeCountAgg> createWriter() {
    return new PostgresEventTypeCountAggWriter();
  }

  @Override
  protected String logContextString(EventTypeCountAgg eventTypeCount) {
    return "count type=" + eventTypeCount.getEventType() + " start=" + eventTypeCount.getWindowStart();
  }

}
