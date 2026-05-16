package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.sinks.PermanentJdbcException;
import com.webcharm.pipeline.sinks.PostgresEventTypeCount5mWriter;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.EventTypeCount5m;
import java.io.IOException;
import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes EventTypeCount5m records to Postgres and routes permanent JDBC failures to a DLQ.
 * Mirrors PostgresWriteFunction's fault-handling strategy for the windowed-counts path.
 */
public class PostgresCountWriteFunction extends ProcessFunction<EventTypeCount5m, DlqRecord> {

  private static final Logger log = LoggerFactory.getLogger(PostgresCountWriteFunction.class);

  private transient PostgresEventTypeCount5mWriter writer;
  private transient ObjectMapper mapper;

  /**
   * Initializes the Postgres writer and JSON serializer once per task slot.
   * Called by Flink before the first processElement invocation on this operator instance.
   */
  @Override
  public void open(OpenContext openContext) {
    writer = new PostgresEventTypeCount5mWriter();
    mapper = new ObjectMapper();
  }

  /**
   * Writes the count record to Postgres. On PermanentJdbcException emits a DlqRecord to the main
   * output instead of propagating. Transient IOException propagates to trigger Flink checkpoint replay.
   */
  @Override
  public void processElement(EventTypeCount5m value, Context ctx, Collector<DlqRecord> out) throws IOException {
    try {
      writer.write(value, null);
    } catch (PermanentJdbcException e) {
      log.error("Permanent JDBC failure for count type={} start={}, routing to DLQ: {}",
          value.getEventType(), value.getWindowStart(), e.getMessage(), e);
      out.collect(new DlqRecord(toRawString(value), e.getMessage(), Instant.now()));
    }
  }

  @Override
  public void close() throws Exception {
    if (writer != null) writer.close();
  }

  /** Serializes the count record to JSON for the DLQ raw field; falls back to a label if serialization fails. */
  private String toRawString(EventTypeCount5m value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      return value.getEventType() + "@" + value.getWindowStart();
    }
  }
}
