package com.webcharm.pipeline.eventtype.sinks;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * Verifies close() always releases the owned connection pool, even when closing the prepared
 * statement or the connection throws. A broken socket (the failure that triggers a sink restart)
 * leaves the statement and connection in a state where their close() can throw; if that aborted the
 * pool close, every restart would orphan a HikariCP pool and leak one Postgres connection.
 */
class JdbcWriterBaseCloseTest {

  /** Combines DataSource with AutoCloseable so the base class treats the mock as a pool it owns. */
  private interface CloseablePool extends DataSource, AutoCloseable {}

  /** Minimal concrete writer; close() behavior under test lives entirely in the base class. */
  private static final class MinimalWriter extends JdbcWriterBase<String> {
    MinimalWriter(DataSource ds) {
      super(ds, "SELECT 1", 1);
    }

    @Override
    public List<JdbcWriter.FailedRow<String>> write(String value) throws IOException {
      return List.of();
    }
  }

  @Test
  void close_whenStatementCloseThrows_stillClosesPool() throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement stmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    doThrow(new SQLException("broken socket")).when(stmt).close();

    CloseablePool ds = mock(CloseablePool.class);
    when(ds.getConnection()).thenReturn(conn);

    MinimalWriter writer = new MinimalWriter(ds);
    assertThrows(Exception.class, writer::close);

    verify(conn).close();
    verify(ds).close();
  }

  @Test
  void close_whenConnectionCloseThrows_stillClosesPool() throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement stmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    doThrow(new SQLException("broken socket")).when(conn).close();

    CloseablePool ds = mock(CloseablePool.class);
    when(ds.getConnection()).thenReturn(conn);

    MinimalWriter writer = new MinimalWriter(ds);
    assertThrows(Exception.class, writer::close);

    verify(ds).close();
  }
}
