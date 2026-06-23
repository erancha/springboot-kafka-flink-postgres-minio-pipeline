package com.webcharm.pipeline.userkeys.types;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * A userKeys event after Kafka deserialization. Carries the identifying (userId, key) pair, the
 * numeric value summed downstream, and the event time that drives windowing. Immutable so it can be
 * carried through Flink checkpoint state and across operator boundaries without risk of mutation.
 */
public final class UserKeyEvent implements Serializable {

  private final UUID id;
  private final String userId;
  private final String key;
  private final long value;
  private final Instant eventTime;

  public UserKeyEvent(UUID id, String userId, String key, long value, Instant eventTime) {
    this.id = id;
    this.userId = userId;
    this.key = key;
    this.value = value;
    this.eventTime = eventTime;
  }

  public UUID getId() { return id; }

  public String getUserId() { return userId; }

  public String getKey() { return key; }

  public long getValue() { return value; }

  public Instant getEventTime() { return eventTime; }
}
