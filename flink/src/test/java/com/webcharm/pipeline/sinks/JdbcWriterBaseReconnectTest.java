package com.webcharm.pipeline.sinks;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * Verifies executeWithRetry supplies the current prepared statement to each JdbcOperation
 * invocation, so a subclass binding through the supplied statement uses the reconnected
 * statement after a transient failure instead of reaching into a base-class field.
 */
class JdbcWriterBaseReconnectTest {

  /** Minimal concrete writer that records which statement each operation invocation received. */
  private static final class RecordingWriter extends JdbcWriterBase<String> {
    final AtomicReference<PreparedStatement> lastSeen = new AtomicReference<>();

    RecordingWriter(DataSource ds) {
      super(ds, "SELECT 1");
    }

    @Override
    public void write(String value) throws IOException {
      executeWithRetry(sqlStmt -> {
        lastSeen.set(sqlStmt);
        sqlStmt.executeUpdate();
      });
    }
  }

  @Test
  void executeWithRetry_afterReconnect_passesFreshStatement() throws Exception {
    Connection conn1 = mock(Connection.class);
    Connection conn2 = mock(Connection.class);
    PreparedStatement stmt1 = mock(PreparedStatement.class);
    PreparedStatement stmt2 = mock(PreparedStatement.class);
    when(conn1.prepareStatement(anyString())).thenReturn(stmt1);
    when(conn2.prepareStatement(anyString())).thenReturn(stmt2);
    // First attempt fails transiently (SQLState 08006 = connection failure); retry must succeed.
    when(stmt1.executeUpdate()).thenThrow(new SQLException("connection reset", "08006"));

    DataSource ds = mock(DataSource.class);
    when(ds.getConnection()).thenReturn(conn1, conn2);

    try (RecordingWriter writer = new RecordingWriter(ds)) {
      writer.write("x");

      // The retry must run against the reconnected statement supplied by the base class —
      // not stmt1, which the subclass would hold if it reached into the field directly.
      assertSame(stmt2, writer.lastSeen.get());
    }
    verify(stmt2).executeUpdate();
  }
}
