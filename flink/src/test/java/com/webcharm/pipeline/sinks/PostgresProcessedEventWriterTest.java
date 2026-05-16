package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.ProcessedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Verifies JDBC parameter binding using a mock Connection; no database required. */
@ExtendWith(MockitoExtension.class)
class PostgresProcessedEventWriterTest {

  @Mock Connection conn;
  @Mock PreparedStatement stmt;

  PostgresProcessedEventWriter writer;

  @BeforeEach
  void setUp() throws Exception {
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    writer = new PostgresProcessedEventWriter(conn);
  }

  @Test
  void write_dataEvent_executesInsert() throws Exception {
    ProcessedEvent event = new ProcessedEvent(
        UUID.fromString("00000000-0000-0000-0000-000000000001"),
        "DATA", Instant.parse("2024-01-15T10:00:00Z"), "ui",
        Map.of("key", "value"), null, null, LocalDate.of(2024, 1, 15));

    writer.write(event, null);

    verify(stmt).executeUpdate();
  }

  @Test
  void write_imageEvent_setsObjectKeyAtIndex6() throws Exception {
    String key = "images/2024-01-15/abc.jpg";
    ProcessedEvent event = new ProcessedEvent(
        UUID.fromString("00000000-0000-0000-0000-000000000002"),
        "IMAGE", Instant.parse("2024-01-15T10:00:00Z"), "ui",
        null, null, key, LocalDate.of(2024, 1, 15));

    writer.write(event, null);

    verify(stmt).setString(6, key);
    verify(stmt).executeUpdate();
  }

  /** A JDBC failure must propagate as IOException so Flink can replay from the last checkpoint. */
  @Test
  void write_jdbcFailure_throwsIOException() throws Exception {
    when(stmt.executeUpdate()).thenThrow(new SQLException("connection reset"));
    ProcessedEvent event = new ProcessedEvent(
        UUID.fromString("00000000-0000-0000-0000-000000000003"),
        "DATA", Instant.parse("2024-01-15T10:00:00Z"), "ui",
        null, null, null, LocalDate.of(2024, 1, 15));

    assertThrows(IOException.class, () -> writer.write(event, null));
  }
}
