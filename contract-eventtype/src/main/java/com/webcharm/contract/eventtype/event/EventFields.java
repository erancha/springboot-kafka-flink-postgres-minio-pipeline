package com.webcharm.contract.eventtype.event;

/**
 * Field names of the JSON event envelope exchanged over Kafka. The backend writes these keys when
 * publishing and the Flink job reads them when parsing, so holding them in one place keeps the
 * producer and consumer from drifting into a silent contract mismatch.
 */
public final class EventFields {

  public static final String ID = "id";
  public static final String EVENT_TYPE = "eventType";
  public static final String EVENT_TIME = "eventTime";
  public static final String SOURCE = "source";
  public static final String PAYLOAD = "payload";
  public static final String IMAGE_URL = "imageUrl";
  public static final String IMAGE_OBJECT_KEY = "imageObjectKey";

  private EventFields() {}
}
