package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.ImageSizeBucketCount5m;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC writer for image_size_buckets_5m. Upserts one row per (window_start, bucket) pair.
 */
public class PostgresImageSizeBucketCount5mWriter extends JdbcWriterBase<ImageSizeBucketCount5m> {

  private static final Logger log = LoggerFactory.getLogger(PostgresImageSizeBucketCount5mWriter.class);

  private static final String SQL =
      "INSERT INTO image_size_buckets_5m (window_start, window_end, bucket, image_count, updated_at) "
          + "VALUES (?, ?, ?, ?, ?) "
          + "ON CONFLICT (window_start, bucket) DO UPDATE SET window_end = EXCLUDED.window_end, "
          + "image_count = EXCLUDED.image_count, updated_at = EXCLUDED.updated_at";

  public PostgresImageSizeBucketCount5mWriter() {
    super(envPool(), SQL);
    log.info("PostgresImageSizeBucketCount5mWriter ready");
  }

  PostgresImageSizeBucketCount5mWriter(String url, String user, String password) {
    super(createPool(url, user, password), SQL);
    log.info("PostgresImageSizeBucketCount5mWriter ready");
  }

  PostgresImageSizeBucketCount5mWriter(Connection conn) {
    super(conn, SQL);
    log.info("PostgresImageSizeBucketCount5mWriter ready");
  }

  @Override
  public void write(ImageSizeBucketCount5m value) throws IOException {
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
