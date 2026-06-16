package com.webcharm.pipeline.eventtype.types;

import com.webcharm.contract.eventtype.event.EventTypes;

/**
 * Canonical eventType string values used throughout the Flink job. DATA and IMAGE are taken from the
 * shared event contract, so the backend (producer) and this job (consumer) cannot drift apart. Only
 * UNEXPECTED is local: it is a Flink-side sentinel for an eventType that is neither DATA nor IMAGE
 * and is routed to the DLQ. TEXT is intentionally absent — the backend normalizes it to DATA before
 * the Kafka publish, so Flink never observes it.
 */
public final class EventType {

  public static final String DATA = EventTypes.DATA;

  public static final String IMAGE = EventTypes.IMAGE;

  /** Flink-local sentinel for an eventType that is neither DATA nor IMAGE; routed to the DLQ. */
  public static final String UNEXPECTED = "UNEXPECTED";

  private EventType() {}
}
