package com.webcharm.pipeline.eventtype.functions;

import com.webcharm.pipeline.eventtype.types.DlqRecord;
import com.webcharm.pipeline.eventtype.types.EnrichResult;
import com.webcharm.pipeline.eventtype.types.ImageSizeBucket;
import com.webcharm.pipeline.eventtype.types.ProcessedEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Splits an EnrichResult stream: a success goes to the main output, any failure to the DLQ side
 * output, and each success of known size additionally emits its size bucket to the
 * image-size-bucket side output that feeds the windowed size histogram. Performs no I/O, so it is
 * safe on the operator task thread.
 */
public class EnrichSplitFunction extends ProcessFunction<EnrichResult, ProcessedEvent> {

  /** Side-output tag for image enrichment failures, routed to the DLQ. */
  public static final OutputTag<DlqRecord> UPLOAD_ERROR_TAG =
      new OutputTag<DlqRecord>("minio-upload-error") {};

  /** Side-output tag carrying one size bucket per successfully stored image of known size. */
  public static final OutputTag<ImageSizeBucket> IMAGE_SIZE_BUCKET_TAG =
      new OutputTag<ImageSizeBucket>("image-size-bucket") {};

  @Override
  public void processElement(EnrichResult value, Context ctx, Collector<ProcessedEvent> out) {
    if (value.isSuccess()) {
      out.collect(value.success());
      Long bytes = value.imageBytes();
      if (bytes != null) {
        ctx.output(IMAGE_SIZE_BUCKET_TAG, ImageSizeBucket.of(bytes));
      }
    } else {
      ctx.output(UPLOAD_ERROR_TAG, value.failure());
    }
  }
}
