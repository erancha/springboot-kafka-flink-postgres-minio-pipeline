package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.sinks.PostgresImageSizeBucketCount99mWriter;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.ImageSizeBucketCount99m;

/**
 * Writes ImageSizeBucketCount99m records to Postgres and routes permanent JDBC failures to a DLQ.
 * Mirrors PostgresWriteFunction's fault-handling strategy for the size-histogram path.
 */
public class PostgresImageSizeBucketCount99mWriteFunction
    extends AbstractPostgresWriteFunction<ImageSizeBucketCount99m> {

  public PostgresImageSizeBucketCount99mWriteFunction(DlqStage stage) {
    super(stage);
  }

  @Override
  protected JdbcWriter<ImageSizeBucketCount99m> createWriter() {
    return new PostgresImageSizeBucketCount99mWriter();
  }

  @Override
  protected String logContextString(ImageSizeBucketCount99m count) {
    return "size-bucket=" + count.getBucket() + " start=" + count.getWindowStart();
  }
}
