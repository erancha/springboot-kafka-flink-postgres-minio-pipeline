package com.webcharm.pipeline.sinks;

import com.webcharm.pipeline.types.ImageSizeBucketCountAgg;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC writer for image_size_buckets_agg. Upserts one row per (window_start, bucket) pair.
 */
public class PostgresImageSizeBucketCountAggWriter extends JdbcWriterBase<ImageSizeBucketCountAgg> {

  private static final Logger log = LoggerFactory.getLogger(PostgresImageSizeBucketCountAggWriter.class);

  private static final String SQL =
      "INSERT INTO image_size_buckets_agg (window_start, window_end, bucket, image_count, updated_at) "
          + "VALUES (?, ?, ?, ?, ?) "
          + "ON CONFLICT (window_start, bucket) DO UPDATE SET window_end = EXCLUDED.window_end, "
          + "image_count = EXCLUDED.image_count, updated_at = EXCLUDED.updated_at";

  public PostgresImageSizeBucketCountAggWriter() {
    super(envPool(), SQL, 1);
    log.info("PostgresImageSizeBucketCountAggWriter ready");
  }

  PostgresImageSizeBucketCountAggWriter(String url, String user, String password) {
    super(createPool(url, user, password), SQL, 1);
    log.info("PostgresImageSizeBucketCountAggWriter ready");
  }

  PostgresImageSizeBucketCountAggWriter(Connection conn) {
    super(conn, SQL, 1);
    log.info("PostgresImageSizeBucketCountAggWriter ready");
  }

  @Override
  public List<FailedRow<ImageSizeBucketCountAgg>> write(ImageSizeBucketCountAgg value) throws IOException {
    return bufferRow(value, sqlStmt -> {
      sqlStmt.setTimestamp(1, Timestamp.from(value.getWindowStart()));
      sqlStmt.setTimestamp(2, Timestamp.from(value.getWindowEnd()));
      sqlStmt.setString(3, value.getBucket());
      sqlStmt.setLong(4, value.getImageCount());
      sqlStmt.setTimestamp(5, Timestamp.from(Instant.now()));
    });
  }
}
