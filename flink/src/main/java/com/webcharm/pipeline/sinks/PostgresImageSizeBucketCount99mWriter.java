package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.ImageSizeBucketCount99m;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC writer for image_size_buckets_99m. Upserts one row per (window_start, bucket) pair.
 */
public class PostgresImageSizeBucketCount99mWriter extends JdbcWriterBase<ImageSizeBucketCount99m> {

  private static final Logger log = LoggerFactory.getLogger(PostgresImageSizeBucketCount99mWriter.class);

  private static final String SQL =
      "INSERT INTO image_size_buckets_99m (window_start, window_end, bucket, image_count, updated_at) "
          + "VALUES (?, ?, ?, ?, ?) "
          + "ON CONFLICT (window_start, bucket) DO UPDATE SET window_end = EXCLUDED.window_end, "
          + "image_count = EXCLUDED.image_count, updated_at = EXCLUDED.updated_at";

  public PostgresImageSizeBucketCount99mWriter() {
    super(envPool(), SQL);
    log.info("PostgresImageSizeBucketCount99mWriter ready");
  }

  PostgresImageSizeBucketCount99mWriter(String url, String user, String password) {
    super(createPool(url, user, password), SQL);
    log.info("PostgresImageSizeBucketCount99mWriter ready");
  }

  PostgresImageSizeBucketCount99mWriter(Connection conn) {
    super(conn, SQL);
    log.info("PostgresImageSizeBucketCount99mWriter ready");
  }

  @Override
  public void write(ImageSizeBucketCount99m value) throws IOException {
    executeWithRetry(sqlStmt -> {
      sqlStmt.setTimestamp(1, Timestamp.from(value.getWindowStart()));
      sqlStmt.setTimestamp(2, Timestamp.from(value.getWindowEnd()));
      sqlStmt.setString(3, value.getBucket());
      sqlStmt.setLong(4, value.getImageCount());
      sqlStmt.setTimestamp(5, Timestamp.from(Instant.now()));
      sqlStmt.executeUpdate();
    });
    log.debug("Wrote window count: bucket={} start={} count={}",
        value.getBucket(), value.getWindowStart(), value.getImageCount());
  }
}
