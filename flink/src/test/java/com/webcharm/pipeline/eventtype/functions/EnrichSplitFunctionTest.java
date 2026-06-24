package com.webcharm.pipeline.eventtype.functions;

import static org.junit.jupiter.api.Assertions.*;

import com.webcharm.pipeline.common.dlq.DlqRecord;
import com.webcharm.pipeline.eventtype.types.DlqStage;
import com.webcharm.pipeline.eventtype.types.EnrichResult;
import com.webcharm.pipeline.eventtype.types.ImageSizeBucket;
import com.webcharm.pipeline.eventtype.types.ProcessedEvent;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

/** Verifies EnrichSplitFunction sends a success to the main output and any failure to the side output. */
class EnrichSplitFunctionTest {

  /** Captures the main-output and side-output records produced by a single processElement call. */
  private static final class Harness extends EnrichSplitFunction {
    final List<ProcessedEvent> main = new ArrayList<>();
    final List<DlqRecord> dlq = new ArrayList<>();
    final List<ImageSizeBucket> sizeBuckets = new ArrayList<>();

    void run(EnrichResult r) {
      Context ctx = new Context() {
        @Override public Long timestamp() { return null; }
        @Override public TimerService timerService() { return null; }
        @Override public <X> void output(OutputTag<X> tag, X val) {
          if (val instanceof DlqRecord d) dlq.add(d);
          if (val instanceof ImageSizeBucket b) sizeBuckets.add(b);
        }
      };
      Collector<ProcessedEvent> col = new Collector<>() {
        public void collect(ProcessedEvent e) { main.add(e); }
        public void close() {}
      };
      processElement(r, ctx, col);
    }
  }

  @Test
  void success_goesToMainOutput() {
    ProcessedEvent e = new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, "images/k.jpg", LocalDate.now());
    Harness h = new Harness();
    h.run(EnrichResult.success(e));
    assertEquals(1, h.main.size());
    assertSame(e, h.main.get(0));
    assertTrue(h.dlq.isEmpty());
  }

  @Test
  void successWithKnownSize_emitsSizeSampleBucket() {
    ProcessedEvent e = new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, "images/k.jpg", LocalDate.now());
    Harness h = new Harness();
    h.run(EnrichResult.success(e, 3L * 1024 * 1024));
    assertEquals(1, h.main.size());
    assertEquals(List.of(ImageSizeBucket.UP_TO_5MB), h.sizeBuckets);
  }

  @Test
  void successWithUnknownSize_emitsNoSizeSample() {
    ProcessedEvent e = new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, "images/k.jpg", LocalDate.now());
    Harness h = new Harness();
    h.run(EnrichResult.success(e, null));
    assertEquals(1, h.main.size());
    assertTrue(h.sizeBuckets.isEmpty());
  }

  @Test
  void permanentFailure_goesToSideOutput() {
    Harness h = new Harness();
    h.run(EnrichResult.permanentFailure(new DlqRecord(DlqStage.IMAGE_ENRICH.name(), "raw", "bad", Instant.now())));
    assertTrue(h.main.isEmpty());
    assertEquals(1, h.dlq.size());
  }

  @Test
  void retryableFailure_goesToSideOutput() {
    Harness h = new Harness();
    h.run(EnrichResult.retryableFailure(new DlqRecord(DlqStage.IMAGE_ENRICH.name(), "raw", "transient", Instant.now())));
    assertTrue(h.main.isEmpty());
    assertEquals(1, h.dlq.size());
  }
}
