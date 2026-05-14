package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.EventTypeCount5m;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC writer for event_type_counts_5m. Upserts one row per (window_start, event_type) pair.
 * Three constructors: no-arg (reads env vars), url/user/password (opens JDBC connection),
 * Connection (accepts an already-open connection — used by unit tests and Testcontainers IT).
 */
public class PostgresEventTypeCount5mWriter extends JdbcWriterBase<EventTypeCount5m> {

  private static final Logger log = LoggerFactory.getLogger(PostgresEventTypeCount5mWriter.class);

  private static final String SQL =
      "INSERT INTO event_type_counts_5m (window_start, window_end, event_type, event_count, updated_at) "
          + "VALUES (?, ?, ?, ?, ?) "
          + "ON CONFLICT (window_start, event_type) DO UPDATE SET window_end = EXCLUDED.window_end, "
          + "event_count = EXCLUDED.event_count, updated_at = EXCLUDED.updated_at";

  /** No-arg constructor: reads connection parameters from environment variables. */
  public PostgresEventTypeCount5mWriter() {
    this(envConnection());
  }

  /** Opens a new JDBC connection from the supplied credentials, then delegates to the Connection constructor. */
  PostgresEventTypeCount5mWriter(String url, String user, String password) {
    this(openConnection(url, user, password));
  }

  /** Accepts an already-open connection and prepares the upsert statement. */
  PostgresEventTypeCount5mWriter(Connection conn) {
    super(conn, SQL);
    log.info("PostgresEventTypeCount5mWriter ready");
  }

  @Override
  public void write(EventTypeCount5m value, Context context) throws IOException {
    try {
      stmt.setTimestamp(1, Timestamp.from(value.getWindowStart()));
      stmt.setTimestamp(2, Timestamp.from(value.getWindowEnd()));
      stmt.setString(3, value.getEventType());
      stmt.setLong(4, value.getEventCount());
      stmt.setTimestamp(5, Timestamp.from(Instant.now()));
      stmt.executeUpdate();
      log.debug("Wrote window count: type={} start={} count={}",
          value.getEventType(), value.getWindowStart(), value.getEventCount());
    } catch (Exception e) {
      throw new IOException("Failed to write event type count", e);
    }
  }
}
