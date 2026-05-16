package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.ProcessedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies insert, image-key storage, and ON CONFLICT upsert against a real PostgreSQL instance managed by Testcontainers. */
@Testcontainers
class PostgresProcessedEventWriterIT {

  @Container
  @SuppressWarnings("resource") // lifecycle managed by @Testcontainers JUnit extension
  static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16")
      .withDatabaseName("warehouse")
      .withUsername("postgres")
      .withPassword("postgres");

  @BeforeAll
  static void createSchema() throws Exception {
    try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), "postgres", "postgres");
         Statement st = c.createStatement()) {
      st.execute("""
          CREATE TABLE processed_events (
            id UUID PRIMARY KEY,
            event_type TEXT NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            source TEXT NOT NULL,
            payload JSONB NULL,
            image_object_key TEXT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          )
          """);
    }
  }

  PostgresProcessedEventWriter writer;

  @BeforeEach
  void setUp() {
    writer = new PostgresProcessedEventWriter(pg.getJdbcUrl(), "postgres", "postgres");
  }

  @AfterEach
  void tearDown() throws Exception {
    writer.close();
    try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), "postgres", "postgres");
         Statement st = c.createStatement()) {
      st.execute("DELETE FROM processed_events");
    }
  }

  @Test
  void write_dataEvent_insertsRow() throws Exception {
    UUID id = UUID.fromString("00000000-0000-0000-0000-000000000001");
    ProcessedEvent event = new ProcessedEvent(
        id, "DATA", Instant.parse("2024-01-15T10:00:00Z"), "ui",
        null, null, null, LocalDate.of(2024, 1, 15));

    writer.write(event, null);

    try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), "postgres", "postgres");
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT event_type, source FROM processed_events WHERE id = '" + id + "'")) {
      assertTrue(rs.next());
      assertEquals("DATA", rs.getString("event_type"));
      assertEquals("ui", rs.getString("source"));
    }
  }

  @Test
  void write_imageEvent_storesObjectKey() throws Exception {
    UUID id = UUID.fromString("00000000-0000-0000-0000-000000000002");
    ProcessedEvent event = new ProcessedEvent(
        id, "IMAGE", Instant.parse("2024-01-15T10:00:00Z"), "ui",
        null, null, "images/2024-01-15/" + id + ".jpg", LocalDate.of(2024, 1, 15));

    writer.write(event, null);

    try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), "postgres", "postgres");
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT image_object_key FROM processed_events WHERE id = '" + id + "'")) {
      assertTrue(rs.next());
      assertEquals("images/2024-01-15/" + id + ".jpg", rs.getString("image_object_key"));
    }
  }

  @Test
  void write_duplicateId_updatesExistingRow() throws Exception {
    UUID id = UUID.fromString("00000000-0000-0000-0000-000000000003");
    ProcessedEvent first = new ProcessedEvent(
        id, "DATA", Instant.parse("2024-01-15T10:00:00Z"), "ui",
        null, null, null, LocalDate.of(2024, 1, 15));
    ProcessedEvent second = new ProcessedEvent(
        id, "IMAGE", Instant.parse("2024-01-15T10:01:00Z"), "ui",
        null, null, "images/key.jpg", LocalDate.of(2024, 1, 15));

    writer.write(first, null);
    writer.write(second, null);

    try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), "postgres", "postgres");
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM processed_events WHERE id = '" + id + "'")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }
}
