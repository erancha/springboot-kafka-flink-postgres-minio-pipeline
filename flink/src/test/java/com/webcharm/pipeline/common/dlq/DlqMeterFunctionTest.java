package com.webcharm.pipeline.common.dlq;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.jupiter.api.Test;

/** Verifies DlqMeterFunction passes records through unchanged and counts them per stage name. */
class DlqMeterFunctionTest {

  /** Stand-in stage enum so the meter test stays independent of any one pipeline's DlqStage. */
  private enum Stage { PARSE, ENRICH, SINK }

  /** Subclass that swaps the metric-group-backed counters for inspectable SimpleCounters. */
  private static final class Fn extends DlqMeterFunction<Stage> {
    final Map<String, Counter> probe = new HashMap<>();

    Fn() {
      super(Stage.class);
      for (Stage s : Stage.values()) {
        probe.put(s.name(), new SimpleCounter());
      }
    }

    @Override
    protected Map<String, Counter> createCounters() {
      return probe;
    }
  }

  private static DlqRecord rec(Stage stage) {
    return new DlqRecord(stage.name(), "raw", "err", Instant.now());
  }

  @Test
  void passesRecordThroughUnchanged() {
    Fn fn = new Fn();
    fn.open(DefaultOpenContext.INSTANCE);
    DlqRecord in = rec(Stage.PARSE);
    assertSame(in, fn.map(in));
  }

  @Test
  void countsPerStage() {
    Fn fn = new Fn();
    fn.open(DefaultOpenContext.INSTANCE);
    fn.map(rec(Stage.ENRICH));
    fn.map(rec(Stage.ENRICH));
    fn.map(rec(Stage.PARSE));
    assertEquals(2, fn.probe.get("ENRICH").getCount());
    assertEquals(1, fn.probe.get("PARSE").getCount());
    assertEquals(0, fn.probe.get("SINK").getCount());
  }
}
