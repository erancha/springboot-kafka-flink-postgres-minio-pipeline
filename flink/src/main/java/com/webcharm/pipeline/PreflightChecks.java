package com.webcharm.pipeline;

import com.webcharm.pipeline.config.EnvConfig;
import io.minio.BucketExistsArgs;
import io.minio.MinioClient;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup validation of external dependencies. Fails fast with a clear message when MinIO or
 * Postgres is misconfigured (bad credentials, missing bucket, missing column) so a broken
 * deployment never starts and thrashes the running pipeline.
 */
public final class PreflightChecks {

  private static final Logger log = LoggerFactory.getLogger(PreflightChecks.class);

  private PreflightChecks() {}

  /**
   * Runs every dependency check against the configured environment. Throws IllegalStateException
   * naming the failing component and cause if any check fails.
   */
  public static void run() {
    verifyMinioBucket(
        MinioClient.builder()
            .endpoint(EnvConfig.env("MINIO_ENDPOINT", "http://minio:9000"))
            .credentials(EnvConfig.env("MINIO_ACCESS_KEY", "minio"),
                EnvConfig.env("MINIO_SECRET_KEY", "minio123"))
            .build(),
        EnvConfig.env("MINIO_BUCKET", "images"));

    String url = EnvConfig.env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/warehouse");
    String user = EnvConfig.env("POSTGRES_USER", "postgres");
    String password = EnvConfig.env("POSTGRES_PASSWORD", "postgres");
    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      verifyPostgresSchema(conn);
    } catch (Exception e) {
      throw fail("Postgres", e);
    }

    log.info("Pre-flight checks passed: MinIO bucket and Postgres schema are reachable");
  }

  /**
   * Verifies the target bucket exists, which also exercises endpoint reachability and
   * credentials. Throws IllegalStateException if the bucket is absent or the call fails.
   */
  static void verifyMinioBucket(MinioClient client, String bucket) {
    boolean exists;
    try {
      exists = client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
    } catch (Exception e) {
      throw fail("MinIO", e);
    }
    if (!exists) {
      throw new IllegalStateException(
          "Pre-flight check failed: MinIO bucket '" + bucket + "' does not exist");
    }
  }

  /**
   * Verifies every Postgres table a sink writer targets exposes the columns that writer binds, so
   * schema drift fails the deployment at startup instead of dead-lettering every write. Throws if
   * any table or column is missing.
   */
  static void verifyPostgresSchema(Connection conn) throws Exception {
    try (Statement st = conn.createStatement()) {
      st.execute("SELECT id, event_type, event_time, source, payload, image_object_key "
          + "FROM processed_events WHERE false");
      st.execute("SELECT window_start, window_end, event_type, event_count, updated_at "
          + "FROM event_type_counts_agg WHERE false");
      st.execute("SELECT window_start, window_end, bucket, image_count, updated_at "
          + "FROM image_size_buckets_agg WHERE false");
    }
  }

  private static IllegalStateException fail(String component, Throwable cause) {
    return new IllegalStateException(
        "Pre-flight check failed against " + component + ": " + cause.getMessage(), cause);
  }
}
