package com.webcharm.pipeline.sinks;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import org.junit.jupiter.api.Test;

/**
 * Verifies the JDBC bound: every prepared statement gets a query timeout applied, so a
 * hung Postgres socket cannot block the operator thread past the checkpoint budget.
 */
class JdbcWriterBaseBoundTest {

  /** Minimal concrete writer for exercising the base-class statement preparation. */
  private static final class TestWriter extends JdbcWriterBase<String> {
    TestWriter(Connection conn) {
      super(conn, "SELECT 1", 1);
    }
    @Override
    public java.util.List<JdbcWriter.FailedRow<String>> write(String value) {
      return java.util.List.of();
    }
  }

  @Test
  void prepareStatement_setsQueryTimeout() throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);

    try (TestWriter ignored = new TestWriter(conn)) {
      // construction applies the bounded query timeout
    }

    // default JDBC_QUERY_TIMEOUT_SECS = 20
    verify(ps).setQueryTimeout(20);
  }
}
