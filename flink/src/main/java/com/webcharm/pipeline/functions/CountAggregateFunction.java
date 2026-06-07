package com.webcharm.pipeline.functions;

import org.apache.flink.api.common.functions.AggregateFunction;

/** Counts elements, keeping only the running total as state. */
public class CountAggregateFunction<T> implements AggregateFunction<T, Long, Long> {

  @Override
  public Long createAccumulator() {
    return 0L;
  }

  @Override
  public Long add(T value, Long accumulator) {
    return accumulator + 1;
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
