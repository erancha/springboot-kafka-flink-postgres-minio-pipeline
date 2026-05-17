package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.config.EnvConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared JDBC scaffolding for Postgres sink writers: connection setup, prepared-statement lifecycle,
 * flush (no-op under autoCommit), and close. Subclasses supply the SQL and implement write().
 *
 * Connections are managed by a HikariCP pool (pool size 1 per writer instance, since each Flink
 * parallel slot gets its own writer). The pool validates and replaces connections that have gone
 * stale due to TCP timeouts or Postgres idle-connection culling.
 *
 * Transient JDBC errors (connection reset, deadlock — SQLState 08xxx/40xxx/57xxx) are retried with
 * exponential backoff and a fresh connection before propagating as IOException. Permanent errors
 * (constraint violations, invalid JSONB — all other SQLStates) throw PermanentJdbcException
 * immediately so callers can route to a dead-letter queue without triggering Flink checkpoint replay.
 */
abstract class JdbcWriterBase<T> implements JdbcWriter<T> {

  /** Operation that can throw SQLException, used as the unit of work passed to executeWithRetry. */
  @FunctionalInterface
  interface JdbcOperation {
    void execute() throws SQLException;
  }

  private static final Logger log = LoggerFactory.getLogger(JdbcWriterBase.class);

  private static final long INITIAL_BACKOFF_MS = 1_000;
  private static final int BACKOFF_MULTIPLIER = 2;
  private static final int MAX_RETRIES = 2; // retries after the first attempt
  private static final int MAX_ATTEMPTS = MAX_RETRIES + 1;

  /**
   * Non-null in the production path (pool-backed connection).
   * Null in the test path (Connection passed directly, no pool).
   */
  private final DataSource datasource;
  private final String sql;

  protected Connection conn;
  protected PreparedStatement stmt;

  /**
   * Creates a HikariCP pool from env vars POSTGRES_URL / POSTGRES_USER / POSTGRES_PASSWORD.
   * Pool size 1: each Flink parallel slot owns exactly one writer instance.
   */
  static HikariDataSource envPool() {
    return createPool(
        EnvConfig.env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/warehouse"),
        EnvConfig.env("POSTGRES_USER", "postgres"),
        EnvConfig.env("POSTGRES_PASSWORD", "postgres"));
  }

  /**
   * Creates a HikariCP pool from explicit credentials.
   * Pool size 1; keepalive settings prevent silent connection death from TCP timeouts or
   * server-side idle-connection culling: connectionTimeout=30s, idleTimeout=600s,
   * maxLifetime=1800s, keepaliveTime=60s.
   */
  static HikariDataSource createPool(String url, String user, String password) {
    HikariConfig cfg = new HikariConfig();
    cfg.setJdbcUrl(url);
    cfg.setUsername(user);
    cfg.setPassword(password);
    cfg.setMaximumPoolSize(1);
    cfg.setConnectionTimeout(30_000);
    cfg.setIdleTimeout(600_000);
    cfg.setMaxLifetime(1_800_000);
    cfg.setKeepaliveTime(60_000);
    return new HikariDataSource(cfg);
  }

  /**
   * Production constructor: borrows a connection from the given pool, sets autoCommit,
   * and prepares the subclass-supplied SQL. The pool is owned by this instance and closed in close().
   */
  protected JdbcWriterBase(DataSource datasource, String sql) {
    this.datasource = datasource;
    this.sql = sql;
    try {
      this.conn = datasource.getConnection();
      this.conn.setAutoCommit(true);
      this.stmt = conn.prepareStatement(sql);
      String url = (datasource instanceof HikariDataSource hds) ? hds.getJdbcUrl() : datasource.toString();
      log.info("Connected to {} via connection pool", url);
    } catch (Exception e) {
      if (datasource instanceof AutoCloseable ac) {
        try {
          ac.close();
        } catch (Exception ignored) {
        }
      }
      throw new RuntimeException("Failed to initialize Postgres connection", e);
    }
  }

  /**
   * Test-facing constructor: accepts an already-open Connection (e.g. from Testcontainers),
   * bypassing the pool. datasource is null; close() only closes stmt and conn.
   */
  protected JdbcWriterBase(Connection conn, String sql) {
    this.datasource = null;
    this.sql = sql;
    this.conn = conn;
    try {
      this.conn.setAutoCommit(true);
      this.stmt = conn.prepareStatement(sql);
    } catch (Exception e) {
      throw new RuntimeException("Failed to prepare statement", e);
    }
  }

  /**
   * Executes op, retrying on transient JDBC errors with exponential backoff (1s, 2s, 4s, ...)
   * and a fresh connection before each retry. Permanent errors (constraint violation, invalid JSONB)
   * are not retried and throw PermanentJdbcException immediately so callers can route to a DLQ.
   * Transient failures throw IOException after MAX_ATTEMPTS, triggering Flink checkpoint replay.
   */
  protected void executeWithRetry(JdbcOperation op) throws IOException {
    SQLException lastEx = null;
    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      try {
        op.execute();
        return;
      } catch (SQLException e) {
        if (!isTransient(e)) {
          throw new PermanentJdbcException("Permanent JDBC failure (SQLState=" + e.getSQLState() + ")", e);
        }
        lastEx = e;
        log.warn("Transient JDBC error (attempt {}/{}), SQLState={}: {}",
            attempt + 1, MAX_ATTEMPTS, e.getSQLState(), e.getMessage());
        if (attempt < MAX_ATTEMPTS - 1) {
          long delayMs = INITIAL_BACKOFF_MS * (long) Math.pow(BACKOFF_MULTIPLIER, attempt);
          log.warn("Retrying in {} ms", delayMs);
          try {
            Thread.sleep(delayMs);
            reconnect();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during JDBC retry backoff", ie);
          } catch (Exception reconnectEx) {
            log.warn("Reconnect failed on attempt {}: {}", attempt + 1, reconnectEx.getMessage());
          }
        }
      }
    }
    throw new IOException("JDBC write failed after " + MAX_ATTEMPTS + " attempts", lastEx);
  }

  /**
   * Closes the current statement and connection, then borrows a fresh connection from the pool
   * and re-prepares the statement. No-op in the test path (no pool available).
   */
  private void reconnect() throws Exception {
    if (datasource == null)
      return;
    try {
      stmt.close();
    } catch (Exception ignored) {
    }
    try {
      conn.close();
    } catch (Exception ignored) {
    }
    conn = datasource.getConnection();
    conn.setAutoCommit(true);
    stmt = conn.prepareStatement(sql);
    log.info("Reconnected to Postgres after transient failure");
  }

  /**
   * Returns true for SQLStates that indicate a transient condition worth retrying.
   * 08xxx: connection errors; 40xxx: transaction rollback / deadlock; 57xxx: operator intervention.
   * null SQLState is treated as transient (unknown — optimistically retry).
   */
  private static boolean isTransient(SQLException e) {
    String state = e.getSQLState();
    if (state == null)
      return true;
    return state.startsWith("08") || state.startsWith("40") || state.startsWith("57");
  }

  @Override
  public void close() throws Exception {
    if (stmt != null)
      stmt.close();
    if (conn != null)
      conn.close();
    if (datasource instanceof AutoCloseable ac)
      ac.close();
  }
}
