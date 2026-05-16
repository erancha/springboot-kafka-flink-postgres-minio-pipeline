package com.webcharm.backend.model;

/** Accepted event types. TEXT is a client-facing alias that normalizes to DATA before Kafka publish. */
public enum EventType {
  DATA, IMAGE, TEXT;

  /** Returns the canonical type name written to Kafka; TEXT is rewritten to DATA. */
  public String normalized() {
    return this == TEXT ? DATA.name() : this.name();
  }
}
