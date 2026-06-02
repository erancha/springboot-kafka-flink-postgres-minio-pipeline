package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.EnrichResult;
import com.webcharm.pipeline.types.ProcessedEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Splits an EnrichResult stream: each result goes to exactly one output - a success to
 * the main output, any failure to the DLQ side output. Performs no I/O, so it is safe
 * on the operator task thread.
 */
public class EnrichSplitFunction extends ProcessFunction<EnrichResult, ProcessedEvent> {

  /** Side-output tag for image enrichment failures, routed to the DLQ. */
  public static final OutputTag<DlqRecord> UPLOAD_ERROR_TAG =
      new OutputTag<DlqRecord>("minio-upload-error") {};

  @Override
  public void processElement(EnrichResult value, Context ctx, Collector<ProcessedEvent> out) {
    if (value.isSuccess()) {
      out.collect(value.success());
    } else {
      ctx.output(UPLOAD_ERROR_TAG, value.failure());
    }
  }
}
