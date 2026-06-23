package com.webcharm.pipeline.userkeys.functions;

import com.webcharm.pipeline.userkeys.types.UserKeyEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Sums the value of UserKeyEvents in a window, keeping only the running total as state. Incremental
 * aggregation means per-window state is a single long per active (userId, key), independent of how
 * many events the window holds.
 */
public class SumAggregateFunction implements AggregateFunction<UserKeyEvent, Long, Long> {

  @Override
  public Long createAccumulator() {
    return 0L;
  }

  @Override
  public Long add(UserKeyEvent value, Long accumulator) {
    return accumulator + value.getValue();
  }

  @Override
  public Long getResult(Long accumulator) {
    return accumulator;
  }

  @Override
  public Long merge(Long a, Long b) {
    return a + b;
  }
}
