package com.webcharm.pipeline.userkeys.types;

import java.io.Serializable;
import java.time.Instant;

/**
 * One window's summed value for a (userId, key) pair: the row written to user_key_aggregates. A
 * mutable Flink POJO (no-arg constructor plus getters/setters) so Flink uses its POJO serializer for
 * checkpoint and network transport.
 */
public class UserKeyAgg implements Serializable {
  private Instant windowStart;
  private Instant windowEnd;
  private String userId;
  private String key;
  private long valueSum;

  public UserKeyAgg() {
  }

  public UserKeyAgg(Instant windowStart, Instant windowEnd, String userId, String key, long valueSum) {
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.userId = userId;
    this.key = key;
    this.valueSum = valueSum;
  }

  public Instant getWindowStart() {
    return windowStart;
  }

  public void setWindowStart(Instant windowStart) {
    this.windowStart = windowStart;
  }

  public Instant getWindowEnd() {
    return windowEnd;
  }

  public void setWindowEnd(Instant windowEnd) {
    this.windowEnd = windowEnd;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public long getValueSum() {
    return valueSum;
  }

  public void setValueSum(long valueSum) {
    this.valueSum = valueSum;
  }
}
