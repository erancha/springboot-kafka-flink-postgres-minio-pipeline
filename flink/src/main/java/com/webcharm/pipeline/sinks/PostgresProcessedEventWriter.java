package com.webcharm.pipeline.sinks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.types.ProcessedEvent;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
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

  public PostgresProcessedEventWriter() {
    super(envPool(), SQL);
    this.mapper = new ObjectMapper();
    log.info("PostgresProcessedEventWriter ready");
  }

  PostgresProcessedEventWriter(String url, String user, String password) {
    super(createPool(url, user, password), SQL);
    this.mapper = new ObjectMapper();
    log.info("PostgresProcessedEventWriter ready");
  }

  PostgresProcessedEventWriter(Connection conn) {
    super(conn, SQL);
    this.mapper = new ObjectMapper();
    log.info("PostgresProcessedEventWriter ready");
  }

  @Override
  public void write(ProcessedEvent value) throws IOException {
    String payloadJson = value.getPayload() == null ? null
        : mapper.writeValueAsString(value.getPayload());
    executeWithRetry(sqlStmt -> {
      sqlStmt.setObject(1, value.getId());
      sqlStmt.setString(2, value.getEventType());
      sqlStmt.setTimestamp(3, Timestamp.from(value.getEventTime()));
      sqlStmt.setString(4, value.getSource());
      sqlStmt.setString(5, payloadJson);
      sqlStmt.setString(6, value.getImageObjectKey());
      sqlStmt.setTimestamp(7, Timestamp.from(Instant.now()));
      sqlStmt.executeUpdate();
    });
    log.debug("Wrote event id={} type={}", value.getId(), value.getEventType());
  }
}
