package com.webcharm.contract.eventtype.event;

/**
 * Canonical eventType values carried on the wire. Only the values both the backend and the Flink
 * job must agree on live here; the backend's inbound TEXT alias and the Flink job's UNEXPECTED
 * parse sentinel are each local to the side that owns them and intentionally absent.
 */
public final class EventTypes {

  public static final String DATA = "DATA";
  public static final String IMAGE = "IMAGE";

  /** Whether a raw eventType string is one of the canonical wire values. */
  public static boolean isCanonical(String eventType) {
    return DATA.equals(eventType) || IMAGE.equals(eventType);
  }

  private EventTypes() {}
}
