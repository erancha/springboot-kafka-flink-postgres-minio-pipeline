package com.webcharm.pipeline.types;

/**
 * Canonical eventType string values used throughout the Flink job. The Flink module does not
 * depend on the backend, so it cannot reuse the backend's EventType enum; this class deliberately
 * mirrors that name so a whole-word search for EventType surfaces both ends of the contract. It is
 * the single place a type name is spelled here, so an evolving schema is a one-file change instead
 * of a hunt for string literals. TEXT is intentionally absent: the backend normalizes it to DATA
 * before the Kafka publish, so Flink never observes it.
 */
public final class EventType {

  /** Arbitrary-payload event; written directly to the Postgres processed_events table. */
  public static final String DATA = "DATA";

  /** Image event; routed through the MinIO upload pipeline before the Postgres write. */
  public static final String IMAGE = "IMAGE";

  /** Flink-local sentinel for an eventType that is neither DATA nor IMAGE; routed to the DLQ. */
  public static final String UNEXPECTED = "UNEXPECTED";

  /** Prevents instantiation; this class is a holder for constants only. */
  private EventType() {}
}
