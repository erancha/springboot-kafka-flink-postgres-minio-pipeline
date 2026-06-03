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
 * exponential backoff and a fresh connection, then propagate as IOException once the retry budget
 * is exhausted. Permanent errors (constraint violations, invalid JSONB — all other SQLStates)
 * throw PermanentJdbcException immediately.
 */
abstract class JdbcWriterBase<T> implements JdbcWriter<T> {

  /** A unit of JDBC work performed against the supplied (always current) prepared statement. */
  @FunctionalInterface
  interface JdbcOperation {
    void execute(PreparedStatement sqlStmt) throws SQLException;
  }

  private static final Logger log = LoggerFactory.getLogger(JdbcWriterBase.class);

  private static final long INITIAL_BACKOFF_MS = 1_000;
  private static final int BACKOFF_MULTIPLIER = 2;
  // Bounded so the worst case (attempts x query timeout + reconnect + backoff) stays under the
  // 60 s checkpoint timeout. Default 2 attempts, 8 s query/socket, 5 s connect, 8 s pool borrow.
  private static final int MAX_ATTEMPTS = Math.max(1, EnvConfig.envInt("JDBC_MAX_ATTEMPTS", 2));
  private static final int QUERY_TIMEOUT_SECS = EnvConfig.envInt("JDBC_QUERY_TIMEOUT_SECS", 8);
  private static final int SOCKET_TIMEOUT_SECS = EnvConfig.envInt("JDBC_SOCKET_TIMEOUT_SECS", 8);
  private static final int CONNECT_TIMEOUT_SECS = EnvConfig.envInt("JDBC_CONNECT_TIMEOUT_SECS", 5);
  private static final int POOL_CONNECTION_TIMEOUT_MS =
      EnvConfig.envInt("JDBC_POOL_CONNECTION_TIMEOUT_MS", 8_000);

  /** Non-null when constructed with a pool; null when constructed with a directly-supplied Connection. */
  private final DataSource datasource;
  private final String sqlText;

  private Connection conn;
  private PreparedStatement sqlStmt;

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
   * Creates a HikariCP pool from explicit credentials. Pool size 1; keepalive prevents silent
   * connection death; connectionTimeout, socketTimeout and connectTimeout are bounded so a hung
   * Postgres socket cannot stall the operator past the checkpoint timeout.
   */
  static HikariDataSource createPool(String url, String user, String password) {
    HikariConfig cfg = new HikariConfig();
    cfg.setJdbcUrl(withTimeoutParams(url));
    cfg.setUsername(user);
    cfg.setPassword(password);
    cfg.setMaximumPoolSize(1);
    cfg.setConnectionTimeout(POOL_CONNECTION_TIMEOUT_MS);
    cfg.setIdleTimeout(600_000);
    cfg.setMaxLifetime(1_800_000);
    cfg.setKeepaliveTime(60_000);
    return new HikariDataSource(cfg);
  }

  /** Appends Postgres socketTimeout/connectTimeout (seconds), adding each only if not already present. */
  static String withTimeoutParams(String url) {
    StringBuilder result = new StringBuilder(url);
    String sep = url.contains("?") ? "&" : "?";
    if (!url.contains("socketTimeout=")) {
      result.append(sep).append("socketTimeout=").append(SOCKET_TIMEOUT_SECS);
      sep = "&";
    }
    if (!url.contains("connectTimeout=")) {
      result.append(sep).append("connectTimeout=").append(CONNECT_TIMEOUT_SECS);
    }
    return result.toString();
  }

  /**
   * Borrows a connection from the given pool, enables autoCommit, and prepares the supplied SQL.
   * The pool is owned by this instance and closed in close().
   */
  protected JdbcWriterBase(DataSource datasource, String sqlText) {
    this.datasource = datasource;
    this.sqlText = sqlText;
    try {
      this.conn = datasource.getConnection();
      this.conn.setAutoCommit(true);
      this.sqlStmt = prepareSqlStmt(conn, sqlText);
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
   * Uses an already-open Connection directly, bypassing the pool; datasource is null,
   * so close() closes only sqlStmt and conn.
   */
  protected JdbcWriterBase(Connection conn, String sqlText) {
    this.datasource = null;
    this.sqlText = sqlText;
    this.conn = conn;
    try {
      this.conn.setAutoCommit(true);
      this.sqlStmt = prepareSqlStmt(conn, sqlText);
    } catch (Exception e) {
      throw new RuntimeException("Failed to prepare statement", e);
    }
  }

  /**
   * Executes op against the current prepared statement, retrying transient JDBC errors with
   * exponential backoff (1s, 2s, 4s, ...) and a fresh connection before each retry. Each attempt
   * (including retries after a reconnect) is handed the live statement, so callers must bind
   * through the supplied statement rather than caching it. Permanent errors (constraint
   * violation, invalid JSONB) are not retried and throw PermanentJdbcException immediately.
   * Transient failures throw IOException after MAX_ATTEMPTS attempts.
   */
  protected void executeWithRetry(JdbcOperation op) throws IOException {
    SQLException lastEx = null;
    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      try {
        op.execute(sqlStmt);
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

  /** Prepares a statement and applies the bounded query timeout so a hung query cannot stall. */
  private static PreparedStatement prepareSqlStmt(Connection c, String sqlText) throws SQLException {
    PreparedStatement ps = c.prepareStatement(sqlText);
    ps.setQueryTimeout(QUERY_TIMEOUT_SECS);
    return ps;
  }

  /**
   * Closes the current statement and connection, then borrows a fresh connection from the pool
   * and re-prepares the statement. No-op when there is no pool (datasource is null).
   */
  private void reconnect() throws Exception {
    if (datasource == null)
      return;
    try {
      sqlStmt.close();
    } catch (Exception ignored) {
    }
    try {
      conn.close();
    } catch (Exception ignored) {
    }
    conn = datasource.getConnection();
    conn.setAutoCommit(true);
    sqlStmt = prepareSqlStmt(conn, sqlText);
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

  /** Closes the prepared statement, the connection, and the pool if this instance owns one. */
  @Override
  public void close() throws Exception {
    if (sqlStmt != null)
      sqlStmt.close();
    if (conn != null)
      conn.close();
    if (datasource instanceof AutoCloseable ac)
      ac.close();
  }
}
