package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.EventTypeCount99m;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC writer for event_type_counts_99m. Upserts one row per (window_start, event_type) pair.
 */
public class PostgresEventTypeCount99mWriter extends JdbcWriterBase<EventTypeCount99m> {

  private static final Logger log = LoggerFactory.getLogger(PostgresEventTypeCount99mWriter.class);

  private static final String SQL =
      "INSERT INTO event_type_counts_99m (window_start, window_end, event_type, event_count, updated_at) "
          + "VALUES (?, ?, ?, ?, ?) "
          + "ON CONFLICT (window_start, event_type) DO UPDATE SET window_end = EXCLUDED.window_end, "
          + "event_count = EXCLUDED.event_count, updated_at = EXCLUDED.updated_at";

  public PostgresEventTypeCount99mWriter() {
    super(envPool(), SQL);
    log.info("PostgresEventTypeCount99mWriter ready");
  }

  PostgresEventTypeCount99mWriter(String url, String user, String password) {
    super(createPool(url, user, password), SQL);
    log.info("PostgresEventTypeCount99mWriter ready");
  }

  PostgresEventTypeCount99mWriter(Connection conn) {
    super(conn, SQL);
    log.info("PostgresEventTypeCount99mWriter ready");
  }

  @Override
  public void write(EventTypeCount99m value) throws IOException {
    executeWithRetry(sqlStmt -> {
      sqlStmt.setTimestamp(1, Timestamp.from(value.getWindowStart()));
      sqlStmt.setTimestamp(2, Timestamp.from(value.getWindowEnd()));
      sqlStmt.setString(3, value.getEventType());
      sqlStmt.setLong(4, value.getEventCount());
      sqlStmt.setTimestamp(5, Timestamp.from(Instant.now()));
      sqlStmt.executeUpdate();
    });
    log.debug("Wrote window count: type={} start={} count={}",
        value.getEventType(), value.getWindowStart(), value.getEventCount());
  }
}
