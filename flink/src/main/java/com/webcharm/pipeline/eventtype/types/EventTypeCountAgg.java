package com.webcharm.pipeline.eventtype.types;

import java.io.Serializable;
import java.time.Instant;

public class EventTypeCountAgg implements Serializable {
  private Instant windowStart;
  private Instant windowEnd;
  private String eventType;
  private long eventCount;

  public EventTypeCountAgg() {
  }

  public EventTypeCountAgg(Instant windowStart, Instant windowEnd, String eventType, long eventCount) {
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.eventType = eventType;
    this.eventCount = eventCount;
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

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public long getEventCount() {
    return eventCount;
  }

  public void setEventCount(long eventCount) {
    this.eventCount = eventCount;
  }
}
