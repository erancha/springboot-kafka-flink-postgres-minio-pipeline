package com.webcharm.pipeline.eventtype.functions;

import static org.junit.jupiter.api.Assertions.*;

import com.webcharm.pipeline.eventtype.types.DlqRecord;
import com.webcharm.pipeline.eventtype.types.DlqStage;
import java.time.Instant;
import java.util.EnumMap;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.jupiter.api.Test;

/** Verifies DlqMeterFunction passes records through unchanged and counts them per DlqStage. */
class DlqMeterFunctionTest {

  /** Subclass that swaps the metric-group-backed counters for inspectable SimpleCounters. */
  private static final class Fn extends DlqMeterFunction {
    final EnumMap<DlqStage, Counter> probe = new EnumMap<>(DlqStage.class);

    Fn() {
      for (DlqStage s : DlqStage.values()) {
        probe.put(s, new SimpleCounter());
      }
    }

    @Override
    protected EnumMap<DlqStage, Counter> createCounters() {
      return probe;
    }
  }

  private static DlqRecord rec(DlqStage stage) {
    return new DlqRecord(stage, "raw", "err", Instant.now());
  }

  @Test
  void passesRecordThroughUnchanged() {
    Fn fn = new Fn();
    fn.open(DefaultOpenContext.INSTANCE);
    DlqRecord in = rec(DlqStage.PARSE);
    assertSame(in, fn.map(in));
  }

  @Test
  void countsPerStage() {
    Fn fn = new Fn();
    fn.open(DefaultOpenContext.INSTANCE);
    fn.map(rec(DlqStage.IMAGE_ENRICH));
    fn.map(rec(DlqStage.IMAGE_ENRICH));
    fn.map(rec(DlqStage.PARSE));
    assertEquals(2, fn.probe.get(DlqStage.IMAGE_ENRICH).getCount());
    assertEquals(1, fn.probe.get(DlqStage.PARSE).getCount());
    assertEquals(0, fn.probe.get(DlqStage.DATA_POSTGRES).getCount());
  }
}
