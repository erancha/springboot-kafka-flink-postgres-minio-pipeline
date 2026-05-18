package com.webcharm.pipeline.types;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the wire values of the EventType constants. These strings must match the canonical
 * eventType names the backend writes to Kafka and the values Postgres stores, so a rename that
 * silently drifts from the producer is caught here rather than in production.
 */
class EventTypeTest {

  /** DATA is the canonical type the backend publishes for arbitrary-payload events. */
  @Test
  void data_hasCanonicalWireValue() {
    assertEquals("DATA", EventType.DATA);
  }

  /** IMAGE is the canonical type the backend publishes for image events. */
  @Test
  void image_hasCanonicalWireValue() {
    assertEquals("IMAGE", EventType.IMAGE);
  }

  /** UNEXPECTED is the Flink-local sentinel for an eventType that is neither DATA nor IMAGE. */
  @Test
  void unexpected_hasSentinelValue() {
    assertEquals("UNEXPECTED", EventType.UNEXPECTED);
  }
}
