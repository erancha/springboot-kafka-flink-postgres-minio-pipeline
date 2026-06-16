package com.webcharm.pipeline.sinks;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the batching buffer in JdbcWriterBase: rows accumulate per connection, flush as one
 * committed batch, and a permanent batch failure replays row-by-row to isolate the poison row.
 */
class JdbcWriterBaseBatchTest {

  /** Minimal concrete writer over String; binds the value as the single statement parameter. */
  private static final class TestWriter extends JdbcWriterBase<String> {
    TestWriter(Connection conn, int batchSize) {
      super(conn, "INSERT INTO t(v) VALUES (?)", batchSize);
    }

    @Override
    public List<JdbcWriter.FailedRow<String>> write(String value) throws java.io.IOException {
      return bufferRow(value, stmt -> stmt.setString(1, value));
    }
  }

  Connection conn;
  PreparedStatement stmt;

  @BeforeEach
  void setUp() throws Exception {
    conn = mock(Connection.class);
    stmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    when(stmt.getConnection()).thenReturn(conn);
  }

  @Test
  void construction_disablesAutoCommit() throws Exception {
    try (TestWriter w = new TestWriter(conn, 5)) {
      verify(conn).setAutoCommit(false);
    }
  }

  @Test
  void buffersUntilBatchSize_thenFlushesOnceAndCommits() throws Exception {
    try (TestWriter w = new TestWriter(conn, 3)) {
      assertTrue(w.write("a").isEmpty());
      assertTrue(w.write("b").isEmpty());
      verify(stmt, never()).executeBatch();
      verify(conn, never()).commit();

      assertTrue(w.write("c").isEmpty());

      verify(stmt, times(3)).addBatch();
      verify(stmt, times(1)).executeBatch();
      verify(conn, times(1)).commit();
    }
  }

  @Test
  void flush_writesBufferedRemainder() throws Exception {
    try (TestWriter w = new TestWriter(conn, 100)) {
      w.write("a");
      w.write("b");
      verify(stmt, never()).executeBatch();

      assertTrue(w.flush().isEmpty());

      verify(stmt, times(2)).addBatch();
      verify(stmt).executeBatch();
      verify(conn).commit();
    }
  }

  @Test
  void flush_emptyBuffer_isNoOp() throws Exception {
    try (TestWriter w = new TestWriter(conn, 5)) {
      assertTrue(w.flush().isEmpty());
      verify(stmt, never()).executeBatch();
      verify(conn, never()).commit();
    }
  }

  /** A permanent batch failure replays the batch row-by-row; the offending row is returned for DLQ. */
  @Test
  void permanentBatchFailure_isolatesPoisonRow_andReturnsIt() throws Exception {
    try (TestWriter w = new TestWriter(conn, 3)) {
      // 23514 = check_violation (permanent). Batch fails, then per-row replay: a ok, b poison, c ok.
      when(stmt.executeBatch())
          .thenThrow(new BatchUpdateException("violation", "23514", new int[] {}));
      when(stmt.executeUpdate())
          .thenReturn(1)
          .thenThrow(new SQLException("violation", "23514"))
          .thenReturn(1);

      w.write("a");
      w.write("b");
      List<JdbcWriter.FailedRow<String>> failed = w.write("c");

      assertEquals(1, failed.size());
      assertEquals("b", failed.get(0).value());
    }
  }
}
