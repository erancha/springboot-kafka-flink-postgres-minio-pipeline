package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.sinks.PermanentJdbcException;
import com.webcharm.pipeline.sinks.PostgresProcessedEventWriter;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.ProcessedEvent;
import java.io.IOException;
import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes ProcessedEvents to Postgres and routes permanent JDBC failures to a DLQ instead of
 * propagating them as IOException. Permanent failures (constraint violations, invalid JSONB) cannot
 * be fixed by Flink checkpoint replay — emitting them as DlqRecords prevents an infinite restart loop.
 * Transient failures still propagate as IOException so Flink can replay from the last checkpoint.
 */
public class PostgresWriteFunction extends ProcessFunction<ProcessedEvent, DlqRecord> {

  private static final Logger log = LoggerFactory.getLogger(PostgresWriteFunction.class);

  private transient PostgresProcessedEventWriter writer;
  private transient ObjectMapper mapper;

  /**
   * Initializes the Postgres writer and JSON serializer once per task slot.
   * Called by Flink before the first processElement invocation on this operator instance.
   */
  @Override
  public void open(OpenContext openContext) {
    writer = new PostgresProcessedEventWriter();
    mapper = new ObjectMapper();
  }

  /**
   * Writes the event to Postgres. On PermanentJdbcException emits a DlqRecord to the main output
   * instead of propagating. Transient IOException propagates to trigger Flink checkpoint replay.
   */
  @Override
  public void processElement(ProcessedEvent value, Context ctx, Collector<DlqRecord> out) throws IOException {
    try {
      writer.write(value, null);
    } catch (PermanentJdbcException e) {
      log.error("Permanent JDBC failure for event id={}, routing to DLQ: {}", value.getId(), e.getMessage(), e);
      out.collect(new DlqRecord(toRawString(value), e.getMessage(), Instant.now()));
    }
  }

  @Override
  public void close() throws Exception {
    if (writer != null) writer.close();
  }

  /** Serializes the event to JSON for the DLQ raw field; falls back to the event ID if serialization fails. */
  private String toRawString(ProcessedEvent value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      return value.getId().toString();
    }
  }
}
