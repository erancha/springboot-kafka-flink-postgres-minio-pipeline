package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.sinks.PostgresImageSizeBucketCount5mWriter;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.ImageSizeBucketCount5m;

/**
 * Writes ImageSizeBucketCount5m records to Postgres and routes permanent JDBC failures to a DLQ.
 * Mirrors PostgresWriteFunction's fault-handling strategy for the size-histogram path.
 */
public class PostgresImageSizeBucketCount5mWriteFunction
    extends AbstractPostgresWriteFunction<ImageSizeBucketCount5m> {

  public PostgresImageSizeBucketCount5mWriteFunction(DlqStage stage) {
    super(stage);
  }

  @Override
  protected JdbcWriter<ImageSizeBucketCount5m> createWriter() {
    return new PostgresImageSizeBucketCount5mWriter();
  }

  @Override
  protected String logContextString(ImageSizeBucketCount5m count) {
    return "size-bucket=" + count.getBucket() + " start=" + count.getWindowStart();
  }
}
