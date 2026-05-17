package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.sinks.PermanentJdbcException;
import com.webcharm.pipeline.types.DlqRecord;
import java.io.IOException;
import java.time.Instant;
import com.webcharm.pipeline.sinks.JdbcWriter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Template Method base for ProcessFunctions that write to Postgres and route permanent JDBC
 * failures to a DLQ. Owns open/processElement/close and DLQ emission;
 * subclasses supply the writer and a log-context string.
 */
abstract class AbstractPostgresWriteFunction<T> extends ProcessFunction<T, DlqRecord> {

  private static final Logger log = LoggerFactory.getLogger(AbstractPostgresWriteFunction.class);

  private transient JdbcWriter<T> writer;
  private transient ObjectMapper mapper;

  /** Returns a new writer instance; called once per task slot during open(). */
  protected abstract JdbcWriter<T> createWriter();

  /** Returns a log-friendly string identifying record (e.g. "event id=abc" or "count type=DATA start=..."). */
  protected abstract String logContextString(T record);

  /** Initializes the writer and JSON serializer once per task slot. */
  @Override
  public void open(OpenContext openContext) {
    writer = createWriter();
    mapper = new ObjectMapper();
  }

  /**
   * Writes record to Postgres. On PermanentJdbcException emits a JSON DlqRecord to the main output
   * instead of propagating, so Flink does not replay an unfixable record. Transient IOException
   * propagates to trigger checkpoint replay.
   */
  @Override
  public void processElement(T record, Context ctx, Collector<DlqRecord> out) throws IOException {
    try {
      writer.write(record);
    } catch (PermanentJdbcException e) {
      log.error("Permanent JDBC failure for {}, routing to DLQ: {}", logContextString(record), e.getMessage(), e);
      out.collect(new DlqRecord(mapper.writeValueAsString(record), e.getMessage(), Instant.now()));
    }
  }

  /** Closes the writer and releases its JDBC connection. */
  @Override
  public void close() throws Exception {
    if (writer != null)
      writer.close();
  }
}
