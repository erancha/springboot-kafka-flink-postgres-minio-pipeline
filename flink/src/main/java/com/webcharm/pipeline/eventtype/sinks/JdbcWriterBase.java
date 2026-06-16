package com.webcharm.pipeline.eventtype.sinks;

import com.webcharm.pipeline.eventtype.config.EnvConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared JDBC scaffolding for Postgres sink writers: connection setup, prepared-statement lifecycle,
 * and a per-connection write buffer flushed as one committed batch. Subclasses supply the SQL and a
 * per-row parameter binder via bufferRow().
 *
 * Each Flink parallel slot owns one writer, one HikariCP connection (pool size 1), and one buffer.
 * Rows accumulate until batchSize, then flush() binds them into a single JDBC batch, executes it, and
 * commits once — collapsing N round-trips and N commit fsyncs into one (reWriteBatchedInserts on the
 * URL rewrites the inserts into a single multi-row statement). autoCommit is off so each batch is one
 * transaction. At-least-once relies on the caller flushing before a checkpoint completes; the
 * subclasses' idempotent upserts make replay safe.
 *
 * Transient JDBC errors (SQLState 08xxx/40xxx/57xxx) retry the whole batch with exponential backoff
 * and a fresh connection, then propagate as IOException. A permanent batch failure (other SQLStates)
 * replays the batch row-by-row so the good rows still commit and the offending rows are returned for
 * dead-letter routing rather than failing the whole batch.
 */
abstract class JdbcWriterBase<T> implements JdbcWriter<T> {

  /** Binds one record's parameters onto the supplied (always current) prepared statement. */
  @FunctionalInterface
  interface JdbcOperation {
    void execute(PreparedStatement sqlStmt) throws SQLException;
  }

  /** A buffered row: the original value (for DLQ routing) and the binder that sets its parameters. */
  private record Pending<T>(T value, JdbcOperation binder) {
  }

  private static final Logger log = LoggerFactory.getLogger(JdbcWriterBase.class);

  private static final long INITIAL_BACKOFF_MS = 1_000;
  private static final int BACKOFF_MULTIPLIER = 2;
  // Bounded so the worst case (attempts x query timeout + reconnect + backoff) stays under the
  // 60 s checkpoint timeout. Default 2 attempts, 20 s query/socket (JDBC_*_TIMEOUT_SECS),
  // 5 s connect, 8 s pool borrow => ~49 s worst case. The 20 s timeout lets a batch ride out a
  // Postgres checkpoint I/O spike; the prior 8 s tripped on those spikes and forced a sink restart.
  // Raising query/socket past ~25 s breaches the 60 s budget unless JDBC_MAX_ATTEMPTS is also cut.
  private static final int MAX_ATTEMPTS = Math.max(1, EnvConfig.envInt("JDBC_MAX_ATTEMPTS", 2));
  private static final int QUERY_TIMEOUT_SECS = EnvConfig.envInt("JDBC_QUERY_TIMEOUT_SECS", 20);
  private static final int SOCKET_TIMEOUT_SECS = EnvConfig.envInt("JDBC_SOCKET_TIMEOUT_SECS", 20);
  private static final int CONNECT_TIMEOUT_SECS = EnvConfig.envInt("JDBC_CONNECT_TIMEOUT_SECS", 5);
  private static final int POOL_CONNECTION_TIMEOUT_MS = EnvConfig.envInt("JDBC_POOL_CONNECTION_TIMEOUT_MS", 8_000);

  /** Non-null when constructed with a pool; null when constructed with a directly-supplied Connection. */
  private final DataSource datasource;
  private final String sqlText;
  private final int batchSize;
  /** Rows buffered since the last flush, in arrival order. Bounded by batchSize except on close/checkpoint flush. */
  private final List<Pending<T>> pending = new ArrayList<>();

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
    cfg.setJdbcUrl(withBatchRewrite(withTimeoutParams(url)));
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
   * Appends reWriteBatchedInserts=true unless already present. This is what lets the Postgres driver
   * rewrite a batch of single-row INSERTs into one multi-row statement, so a flush is one round-trip.
   */
  static String withBatchRewrite(String url) {
    if (url.contains("reWriteBatchedInserts=")) {
      return url;
    }
    return url + (url.contains("?") ? "&" : "?") + "reWriteBatchedInserts=true";
  }

  /**
   * Borrows a connection from the given pool, disables autoCommit (so each flush is one transaction),
   * and prepares the supplied SQL. The pool is owned by this instance and closed in close().
   */
  protected JdbcWriterBase(DataSource datasource, String sqlText, int batchSize) {
    this.datasource = datasource;
    this.sqlText = sqlText;
    this.batchSize = Math.max(1, batchSize);
    try {
      this.conn = datasource.getConnection();
      this.conn.setAutoCommit(false);
      this.sqlStmt = prepareSqlStmt(conn, sqlText);
      String url = (datasource instanceof HikariDataSource hds) ? hds.getJdbcUrl() : datasource.toString();
      log.info("Connected to {} via connection pool (batchSize={})", url, this.batchSize);
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
  protected JdbcWriterBase(Connection conn, String sqlText, int batchSize) {
    this.datasource = null;
    this.sqlText = sqlText;
    this.batchSize = Math.max(1, batchSize);
    this.conn = conn;
    try {
      this.conn.setAutoCommit(false);
      this.sqlStmt = prepareSqlStmt(conn, sqlText);
    } catch (Exception e) {
      throw new RuntimeException("Failed to prepare statement", e);
    }
  }

  /**
   * Buffers one row, supplying the binder that will set its parameters at flush time. Flushes when the
   * buffer reaches batchSize. Returns the rows that permanently failed during a triggered flush
   * (empty otherwise), so the caller can route them to a dead-letter sink.
   */
  protected final List<FailedRow<T>> bufferRow(T value, JdbcOperation binder) throws IOException {
    pending.add(new Pending<>(value, binder));
    if (pending.size() >= batchSize) {
      return flush();
    }
    return List.of();
  }

  /**
   * Binds and executes all buffered rows as one committed batch. A transient failure retries the
   * whole batch (see executeWithRetry); a permanent failure replays the batch row-by-row so the good
   * rows still commit and the offending rows are returned for DLQ routing. The buffer is drained
   * regardless — on a propagated transient error the rows replay from the source after restart.
   */
  @Override
  public List<FailedRow<T>> flush() throws IOException {
    if (pending.isEmpty()) {
      return List.of();
    }
    List<Pending<T>> batch = List.copyOf(pending);
    pending.clear();
    try {
      executeWithRetry(stmt -> {
        stmt.clearBatch();
        for (Pending<T> p : batch) {
          p.binder().execute(stmt);
          stmt.addBatch();
        }
        stmt.executeBatch();
        stmt.getConnection().commit();
      });
    } catch (PermanentJdbcException e) {
      return isolatePoison(batch);
    }
    return List.of();
  }

  /**
   * Replays a batch that hit a permanent failure one row at a time so the good rows still commit;
   * returns the rows whose individual write also failed permanently. A transient failure here
   * propagates (after the retry budget) so the surviving rows replay from the source after restart.
   */
  private List<FailedRow<T>> isolatePoison(List<Pending<T>> batch) throws IOException {
    List<FailedRow<T>> failed = new ArrayList<>();
    for (Pending<T> p : batch) {
      try {
        executeWithRetry(stmt -> {
          p.binder().execute(stmt);
          stmt.executeUpdate();
          stmt.getConnection().commit();
        });
      } catch (PermanentJdbcException e) {
        log.warn("Permanent JDBC failure on an isolated row (SQLState in cause); routing to DLQ: {}",
            e.getMessage());
        failed.add(new FailedRow<>(p.value(), e.getMessage()));
      }
    }
    return failed;
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
   * and re-prepares the statement. No-op when there is no pool (datasource is null). The closed
   * connection discards any transaction left aborted by the failure that triggered the reconnect.
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
    conn.setAutoCommit(false);
    sqlStmt = prepareSqlStmt(conn, sqlText);
    log.info("Reconnected to Postgres after transient failure");
  }

  /**
   * Returns true for failures worth retrying. executeBatch() throws a BatchUpdateException
   * (java.sql contract) whose underlying SQLExceptions are linked via getNextException(); a
   * connection loss is also reachable through getCause(). Both chains are walked so a transient
   * marker is honored wherever the driver places it, not only on the outermost exception.
   *
   * Transient: SQLState 08 (connection), 40 (rollback / deadlock), 57 (operator intervention),
   * null (unknown — optimistically retry), or a bare SocketException / EOFException. Constraint and
   * data errors (23 / 22) match none of these and stay permanent, so poison rows still reach the DLQ.
   */
  private static boolean isTransient(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof SQLException sqlEx) {
        for (SQLException s = sqlEx; s != null; s = s.getNextException()) {
          String state = s.getSQLState();
          if (state == null
              || state.startsWith("08") || state.startsWith("40") || state.startsWith("57")) {
            return true;
          }
        }
      }
      if (t instanceof java.net.SocketException || t instanceof java.io.EOFException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Closes the prepared statement, the connection, and the pool if this instance owns one. Each step
   * runs even if an earlier one throws, so the owned pool is always released: the failure that
   * triggers a sink restart (broken socket) leaves the statement and connection in a state where
   * close() can throw, and aborting before the pool close would orphan a HikariCP pool and leak one
   * Postgres connection per restart. The first failure propagates with the rest as suppressed.
   */
  @Override
  public void close() throws Exception {
    Exception failure = null;
    failure = closeQuietly(sqlStmt, failure);
    failure = closeQuietly(conn, failure);
    if (datasource instanceof AutoCloseable ac) {
      failure = closeQuietly(ac, failure);
    }
    if (failure != null) {
      throw failure;
    }
  }

  /** Closes a resource (no-op if null), folding any thrown exception into the running first-failure. */
  private static Exception closeQuietly(AutoCloseable resource, Exception failure) {
    if (resource == null) {
      return failure;
    }
    try {
      resource.close();
      return failure;
    } catch (Exception e) {
      if (failure == null) {
        return e;
      }
      failure.addSuppressed(e);
      return failure;
    }
  }
}
