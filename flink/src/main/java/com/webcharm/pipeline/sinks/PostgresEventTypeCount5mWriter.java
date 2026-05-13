package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.config.EnvConfig;
import com.webcharm.pipeline.types.EventTypeCount5m;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresEventTypeCount5mWriter implements SinkWriter<EventTypeCount5m> {

  private static final Logger log = LoggerFactory.getLogger(PostgresEventTypeCount5mWriter.class);

  private final Connection conn;
  private final PreparedStatement stmt;

  public PostgresEventTypeCount5mWriter() {
    String url      = EnvConfig.env("POSTGRES_URL",      "jdbc:postgresql://postgres:5432/warehouse");
    String user     = EnvConfig.env("POSTGRES_USER",     "postgres");
    String password = EnvConfig.env("POSTGRES_PASSWORD", "postgres");
    try {
      this.conn = DriverManager.getConnection(url, user, password);
      this.conn.setAutoCommit(true);
      this.stmt = conn.prepareStatement(
          "INSERT INTO event_type_counts_5m (window_start, window_end, event_type, event_count, updated_at) "
              + "VALUES (?, ?, ?, ?, ?) "
              + "ON CONFLICT (window_start, event_type) DO UPDATE SET window_end = EXCLUDED.window_end, "
              + "event_count = EXCLUDED.event_count, updated_at = EXCLUDED.updated_at");
      log.info("PostgresEventTypeCount5mWriter connected to {}", url);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize Postgres connection", e);
    }
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

  @Override
  public void flush(boolean endOfInput) {
    // autoCommit=true
  }

  @Override
  public void close() throws Exception {
    if (stmt != null) stmt.close();
    if (conn  != null) conn.close();
  }
}
