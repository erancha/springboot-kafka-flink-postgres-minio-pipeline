package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.config.EnvConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared JDBC scaffolding for Postgres sink writers: connection setup, prepared-statement lifecycle,
 * flush (no-op under autoCommit), and close. Subclasses supply the SQL and implement write().
 */
abstract class JdbcWriterBase<T> implements SinkWriter<T> {

  private static final Logger log = LoggerFactory.getLogger(JdbcWriterBase.class);

  protected final Connection conn;
  protected final PreparedStatement stmt;

  /** Opens a JDBC connection from env vars POSTGRES_URL / POSTGRES_USER / POSTGRES_PASSWORD. */
  static Connection envConnection() {
    return openConnection(
        EnvConfig.env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/warehouse"),
        EnvConfig.env("POSTGRES_USER", "postgres"),
        EnvConfig.env("POSTGRES_PASSWORD", "postgres"));
  }

  /** Opens a JDBC connection from explicit credentials; wraps any SQLException in RuntimeException. */
  static Connection openConnection(String url, String user, String password) {
    try {
      Connection c = DriverManager.getConnection(url, user, password);
      log.info("Connected to {}", url);
      return c;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize Postgres connection", e);
    }
  }

  /**
   * Configures autoCommit and prepares the subclass-supplied SQL statement.
   * Throws RuntimeException if statement preparation fails.
   */
  protected JdbcWriterBase(Connection conn, String sql) {
    this.conn = conn;
    try {
      this.conn.setAutoCommit(true);
      this.stmt = conn.prepareStatement(sql);
    } catch (Exception e) {
      throw new RuntimeException("Failed to prepare statement", e);
    }
  }

  @Override
  public void flush(boolean endOfInput) {
    // autoCommit=true; nothing to flush
  }

  @Override
  public void close() throws Exception {
    if (stmt != null) stmt.close();
    if (conn  != null) conn.close();
  }
}
