package com.webcharm.pipeline.userkeys.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.webcharm.pipeline.userkeys.types.UserKeyEvent;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Unit tests for SumAggregateFunction — the incremental value sum used by the windowed aggregation. */
class SumAggregateFunctionTest {

  private static UserKeyEvent event(long value) {
    return new UserKeyEvent(UUID.randomUUID(), "user-1", "A", value, Instant.parse("2024-01-15T10:00:00Z"));
  }

  @Test
  void createAccumulator_isZero() {
    assertEquals(0L, new SumAggregateFunction().createAccumulator());
  }

  @Test
  void add_accumulatesEventValues() {
    SumAggregateFunction fn = new SumAggregateFunction();
    long acc = fn.createAccumulator();
    acc = fn.add(event(100), acc);
    acc = fn.add(event(200), acc);
    assertEquals(300L, fn.getResult(acc));
  }

  @Test
  void merge_sumsPartialAccumulators() {
    SumAggregateFunction fn = new SumAggregateFunction();
    assertEquals(500L, fn.merge(200L, 300L));
  }

  @Test
  void add_handlesNegativeAndZeroValues() {
    SumAggregateFunction fn = new SumAggregateFunction();
    long acc = fn.createAccumulator();
    acc = fn.add(event(0), acc);
    acc = fn.add(event(-50), acc);
    acc = fn.add(event(75), acc);
    assertEquals(25L, fn.getResult(acc));
  }
}
