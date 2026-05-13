package com.webcharm.pipeline.sinks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.config.EnvConfig;
import com.webcharm.pipeline.types.ProcessedEvent;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC writer for processed_events. Payload sanitization is delegated to the parameterized
 * prepared statement (SQL injection prevention); no additional cleanPayload step is needed.
 * Three constructors: no-arg (reads env vars), url/user/password (opens JDBC connection),
 * Connection (accepts an already-open connection — used by mock unit tests and Testcontainers IT).
 */
public class PostgresProcessedEventWriter implements SinkWriter<ProcessedEvent> {

  private static final Logger log = LoggerFactory.getLogger(PostgresProcessedEventWriter.class);

  private final ObjectMapper mapper;
  private final Connection conn;
  private final PreparedStatement stmt;

  /** No-arg constructor: reads connection parameters from environment variables. */
  public PostgresProcessedEventWriter() {
    this(
        EnvConfig.env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/warehouse"),
        EnvConfig.env("POSTGRES_USER", "postgres"),
        EnvConfig.env("POSTGRES_PASSWORD", "postgres"));
  }

  /** Opens a new JDBC connection from the supplied credentials, then delegates to the Connection constructor. */
  PostgresProcessedEventWriter(String url, String user, String password) {
    this(openConnection(url, user, password));
  }

  /** Accepts an already-open connection and prepares the upsert statement. */
  PostgresProcessedEventWriter(Connection conn) {
    this.mapper = new ObjectMapper();
    this.conn = conn;
    try {
      this.conn.setAutoCommit(true);
      this.stmt = conn.prepareStatement(
          "INSERT INTO processed_events (id, event_type, event_time, source, payload, image_object_key, inserted_at) "
              + "VALUES (?, ?, ?, ?, ?::jsonb, ?, ?) "
              + "ON CONFLICT (id) DO UPDATE SET event_type = EXCLUDED.event_type, event_time = EXCLUDED.event_time, "
              + "source = EXCLUDED.source, payload = EXCLUDED.payload, image_object_key = EXCLUDED.image_object_key");
      log.info("PostgresProcessedEventWriter ready");
    } catch (Exception e) {
      throw new RuntimeException("Failed to prepare statement", e);
    }
  }

  /** Opens a JDBC connection; wraps any SQLException in a RuntimeException. */
  private static Connection openConnection(String url, String user, String password) {
    try {
      Connection c = DriverManager.getConnection(url, user, password);
      log.info("PostgresProcessedEventWriter connected to {}", url);
      return c;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize Postgres connection", e);
    }
  }

  @Override
  public void write(ProcessedEvent value, Context context) throws IOException {
    try {
      String payloadJson = value.getPayload() == null ? null
          : mapper.writeValueAsString(value.getPayload());
      stmt.setObject(1, value.getId());
      stmt.setString(2, value.getEventType());
      stmt.setTimestamp(3, Timestamp.from(value.getEventTime()));
      stmt.setString(4, value.getSource());
      stmt.setString(5, payloadJson);
      stmt.setString(6, value.getImageObjectKey());
      stmt.setTimestamp(7, Timestamp.from(Instant.now()));
      stmt.executeUpdate();
      log.debug("Wrote event id={} type={}", value.getId(), value.getEventType());
    } catch (Exception e) {
      throw new IOException("Failed to write processed event", e);
    }
  }

  @Override
  public void flush(boolean endOfInput) {
    // autoCommit=true; nothing to flush
  }

  @Override
  public void close() throws Exception {
    if (stmt != null)
      stmt.close();
    if (conn != null)
      conn.close();
  }
}
