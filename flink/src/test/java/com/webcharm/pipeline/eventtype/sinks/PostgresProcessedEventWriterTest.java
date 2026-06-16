package com.webcharm.pipeline.eventtype.sinks;

import com.webcharm.pipeline.eventtype.types.ProcessedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Verifies batched JDBC parameter binding and failure routing using a mock Connection; no database required. */
@ExtendWith(MockitoExtension.class)
class PostgresProcessedEventWriterTest {

  @Mock Connection conn;
  @Mock PreparedStatement stmt;

  PostgresProcessedEventWriter writer;

  private static ProcessedEvent event(String idLast, String type, String imageKey) {
    return new ProcessedEvent(
        UUID.fromString("00000000-0000-0000-0000-00000000000" + idLast),
        type, Instant.parse("2024-01-15T10:00:00Z"), "ui",
        Map.of("key", "value"), null, imageKey, LocalDate.of(2024, 1, 15));
  }

  @BeforeEach
  void setUp() throws Exception {
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    writer = new PostgresProcessedEventWriter(conn);
  }

  /** A single buffered row (batchSize 1) flushes as a committed batch. */
  @Test
  void write_dataEvent_executesBatchAndCommits() throws Exception {
    when(stmt.getConnection()).thenReturn(conn);

    writer.write(event("1", "DATA", null));

    verify(stmt).executeBatch();
    verify(conn).commit();
  }

  @Test
  void write_imageEvent_setsObjectKeyAtIndex6() throws Exception {
    when(stmt.getConnection()).thenReturn(conn);
    String key = "images/2024-01-15/abc.jpg";

    writer.write(event("2", "IMAGE", key));

    verify(stmt).setString(6, key);
    verify(stmt).executeBatch();
  }

  /** Transient JDBC failure (null SQLState → treated as unknown/transient) propagates as IOException for Flink replay. */
  @Test
  void write_transientJdbcFailure_throwsIOException() throws Exception {
    when(stmt.executeBatch()).thenThrow(new SQLException("connection reset"));

    assertThrows(IOException.class, () -> writer.write(event("3", "DATA", null)));
  }

  /** Permanent batch failure replays row-by-row; the offending record is returned for DLQ routing, not thrown. */
  @Test
  void write_permanentJdbcFailure_returnsFailedRow() throws Exception {
    // SQLState 23514 = check_violation (permanent; does not start with 08/40/57)
    when(stmt.executeBatch()).thenThrow(new BatchUpdateException("violation", "23514", new int[] {}));
    when(stmt.executeUpdate()).thenThrow(new SQLException("check constraint violated", "23514"));
    ProcessedEvent e = event("4", "DATA", null);

    List<JdbcWriter.FailedRow<ProcessedEvent>> failed = writer.write(e);

    assertEquals(1, failed.size());
    assertEquals(e, failed.get(0).value());
  }
}
