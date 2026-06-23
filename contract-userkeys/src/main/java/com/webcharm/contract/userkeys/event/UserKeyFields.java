package com.webcharm.contract.userkeys.event;

/**
 * Field names of the JSON event envelope exchanged over Kafka for the userKeys pipeline. The backend
 * writes these keys when publishing and the Flink job reads them when parsing, so holding them in one
 * place keeps the producer and consumer from drifting into a silent contract mismatch.
 */
public final class UserKeyFields {

  public static final String ID = "id";
  public static final String EVENT_TIME = "eventTime";
  public static final String SOURCE = "source";
  public static final String USER_ID = "userId";
  public static final String KEY = "key";
  public static final String VALUE = "value";

  private UserKeyFields() {}
}
