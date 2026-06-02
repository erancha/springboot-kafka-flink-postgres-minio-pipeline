package com.webcharm.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import com.webcharm.pipeline.functions.EnrichSplitFunction;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.ProcessedEvent;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Verifies buildImagePipeline on a local Flink runtime: success to main output, failure to the
 * DLQ side output. Uses a bounded in-memory source and the real async operator + split wiring.
 */
class ImagePipelineIT {

  private static ProcessedEvent img(String url, String key) {
    return new ProcessedEvent(
        UUID.randomUUID(), "IMAGE", Instant.now(), "test",
        null, url, key, LocalDate.now());
  }

  @Test
  void successEventsReachMainOutput() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    env.setParallelism(1);

    // imageObjectKey already set => MinioAsyncImageFunction passes through as success
    // with no network I/O, so this exercises the real async operator + split without Docker.
    DataStream<ProcessedEvent> source = env.fromData(
        img(null, "images/2026-05-17/a.jpg"),
        img(null, "images/2026-05-17/b.jpg"));

    SingleOutputStreamOperator<ProcessedEvent> main = StreamingJob.buildImagePipeline(source);

    List<String> keys = new ArrayList<>();
    try (CloseableIterator<ProcessedEvent> it = main.executeAndCollect()) {
      it.forEachRemaining(e -> keys.add(e.getImageObjectKey()));
    }

    assertEquals(2, keys.size());
    assertTrue(keys.contains("images/2026-05-17/a.jpg"));
    assertTrue(keys.contains("images/2026-05-17/b.jpg"));
  }

  @Test
  void failureEventsReachDlqSideOutput() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    env.setParallelism(1);

    // Neither imageUrl nor imageObjectKey => permanent failure => DLQ side output.
    DataStream<ProcessedEvent> source = env.fromData(img(null, null));

    SingleOutputStreamOperator<ProcessedEvent> main = StreamingJob.buildImagePipeline(source);

    List<DlqRecord> dlq = new ArrayList<>();
    try (CloseableIterator<DlqRecord> it =
        main.getSideOutput(EnrichSplitFunction.UPLOAD_ERROR_TAG).executeAndCollect()) {
      it.forEachRemaining(dlq::add);
    }

    assertEquals(1, dlq.size());
    assertTrue(dlq.get(0).error().contains("neither imageUrl nor imageObjectKey"));
    assertEquals(com.webcharm.pipeline.types.DlqStage.IMAGE_ENRICH, dlq.get(0).stage());
  }
}
