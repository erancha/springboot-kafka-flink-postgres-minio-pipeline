package com.webcharm.pipeline.eventtype.sinks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.eventtype.config.EnvConfig;
import com.webcharm.pipeline.eventtype.types.ProcessedEvent;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC writer for processed_events. Payload sanitization is delegated to the parameterized
 * prepared statement (SQL injection prevention); no additional cleanPayload step is needed.
 */
public class PostgresProcessedEventWriter extends JdbcWriterBase<ProcessedEvent> {

  private static final Logger log = LoggerFactory.getLogger(PostgresProcessedEventWriter.class);

  private static final String SQL =
      "INSERT INTO processed_events (id, event_type, event_time, source, payload, image_object_key, inserted_at) "
          + "VALUES (?, ?, ?, ?, ?::jsonb, ?, ?) "
          + "ON CONFLICT (id) DO UPDATE SET event_type = EXCLUDED.event_type, event_time = EXCLUDED.event_time, "
          + "source = EXCLUDED.source, payload = EXCLUDED.payload, image_object_key = EXCLUDED.image_object_key";

  private final ObjectMapper mapper;

  // High-volume path: batch size from JDBC_BATCH_SIZE (default 500). The aggregate sinks pass 1
  // (per-row) since each window already coalesces to a single upsert.
  public PostgresProcessedEventWriter() {
    super(envPool(), SQL, EnvConfig.envInt("JDBC_BATCH_SIZE", 500));
    this.mapper = new ObjectMapper();
    log.info("PostgresProcessedEventWriter ready");
  }

  PostgresProcessedEventWriter(String url, String user, String password) {
    this(url, user, password, 1);
  }

  PostgresProcessedEventWriter(String url, String user, String password, int batchSize) {
    super(createPool(url, user, password), SQL, batchSize);
    this.mapper = new ObjectMapper();
    log.info("PostgresProcessedEventWriter ready");
  }

  PostgresProcessedEventWriter(Connection conn) {
    super(conn, SQL, 1);
    this.mapper = new ObjectMapper();
    log.info("PostgresProcessedEventWriter ready");
  }

  @Override
  public List<FailedRow<ProcessedEvent>> write(ProcessedEvent value) throws IOException {
    String payloadJson = value.getPayload() == null ? null
        : mapper.writeValueAsString(value.getPayload());
    return bufferRow(value, sqlStmt -> {
      sqlStmt.setObject(1, value.getId());
      sqlStmt.setString(2, value.getEventType());
      sqlStmt.setTimestamp(3, Timestamp.from(value.getEventTime()));
      sqlStmt.setString(4, value.getSource());
      sqlStmt.setString(5, payloadJson);
      sqlStmt.setString(6, value.getImageObjectKey());
      sqlStmt.setTimestamp(7, Timestamp.from(Instant.now()));
    });
  }
}
