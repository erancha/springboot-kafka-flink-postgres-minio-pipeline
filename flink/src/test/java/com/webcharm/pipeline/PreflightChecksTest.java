package com.webcharm.pipeline;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

import io.minio.MinioClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/** Verifies the startup pre-flight checks fail fast on a missing bucket, a failed MinIO call, or a missing Postgres column. */
class PreflightChecksTest {

  @Test
  void minioBucketPresent_passes() throws Exception {
    MinioClient client = mock(MinioClient.class);
    when(client.bucketExists(any())).thenReturn(true);
    assertDoesNotThrow(() -> PreflightChecks.verifyMinioBucket(client, "images"));
  }

  @Test
  void minioBucketMissing_throws() throws Exception {
    MinioClient client = mock(MinioClient.class);
    when(client.bucketExists(any())).thenReturn(false);
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> PreflightChecks.verifyMinioBucket(client, "images"));
    assertTrue(ex.getMessage().contains("images"));
  }

  @Test
  void minioCallFails_throwsWrapped() throws Exception {
    MinioClient client = mock(MinioClient.class);
    when(client.bucketExists(any())).thenThrow(new RuntimeException("connection refused"));
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> PreflightChecks.verifyMinioBucket(client, "images"));
    assertTrue(ex.getMessage().contains("MinIO"));
  }

  @Test
  void postgresSchemaOk_passes() throws Exception {
    Connection conn = mock(Connection.class);
    Statement st = mock(Statement.class);
    when(conn.createStatement()).thenReturn(st);
    when(st.execute(anyString())).thenReturn(false);
    assertDoesNotThrow(() -> PreflightChecks.verifyPostgresSchema(conn));
  }

  @Test
  void postgresColumnMissing_throws() throws Exception {
    Connection conn = mock(Connection.class);
    Statement st = mock(Statement.class);
    when(conn.createStatement()).thenReturn(st);
    when(st.execute(anyString())).thenThrow(new SQLException("column \"x\" does not exist", "42703"));
    assertThrows(SQLException.class, () -> PreflightChecks.verifyPostgresSchema(conn));
  }

  @Test
  void countTableMissing_throws() throws Exception {
    Connection conn = mock(Connection.class);
    Statement st = mock(Statement.class);
    when(conn.createStatement()).thenReturn(st);
    when(st.execute(contains("processed_events"))).thenReturn(false);
    when(st.execute(contains("event_type_counts_agg")))
        .thenThrow(new SQLException("relation \"event_type_counts_agg\" does not exist", "42P01"));
    assertThrows(SQLException.class, () -> PreflightChecks.verifyPostgresSchema(conn));
  }

  @Test
  void imageSizeBucketTableMissing_throws() throws Exception {
    Connection conn = mock(Connection.class);
    Statement st = mock(Statement.class);
    when(conn.createStatement()).thenReturn(st);
    when(st.execute(contains("processed_events"))).thenReturn(false);
    when(st.execute(contains("event_type_counts_agg"))).thenReturn(false);
    when(st.execute(contains("image_size_buckets_agg")))
        .thenThrow(new SQLException("relation \"image_size_buckets_agg\" does not exist", "42P01"));
    assertThrows(SQLException.class, () -> PreflightChecks.verifyPostgresSchema(conn));
  }
}
