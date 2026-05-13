package com.webcharm.pipeline.types;

import java.io.Serializable;
import java.time.Instant;

public class EventTypeCount5m implements Serializable {
  private Instant windowStart;
  private Instant windowEnd;
  private String eventType;
  private long eventCount;

  public EventTypeCount5m() {
  }

  public EventTypeCount5m(Instant windowStart, Instant windowEnd, String eventType, long eventCount) {
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
