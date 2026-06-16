package com.webcharm.pipeline.eventtype.functions;

import com.webcharm.pipeline.eventtype.sinks.JdbcWriter;
import com.webcharm.pipeline.eventtype.sinks.PostgresImageSizeBucketCountAggWriter;
import com.webcharm.pipeline.eventtype.types.DlqStage;
import com.webcharm.pipeline.eventtype.types.ImageSizeBucketCountAgg;

/**
 * Writes ImageSizeBucketCountAgg records to Postgres and routes permanent JDBC failures to a DLQ.
 * Mirrors PostgresWriteFunction's fault-handling strategy for the size-histogram path.
 */
public class PostgresImageSizeBucketCountAggWriteFunction
    extends AbstractPostgresWriteFunction<ImageSizeBucketCountAgg> {

  public PostgresImageSizeBucketCountAggWriteFunction(DlqStage stage) {
    super(stage);
  }

  @Override
  protected JdbcWriter<ImageSizeBucketCountAgg> createWriter() {
    return new PostgresImageSizeBucketCountAggWriter();
  }

  @Override
  protected String logContextString(ImageSizeBucketCountAgg count) {
    return "size-bucket=" + count.getBucket() + " start=" + count.getWindowStart();
  }
}
