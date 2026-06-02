package com.webcharm.pipeline.types;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

/**
 * Event after Kafka deserialization. Carries only the fields written to Postgres. Immutable so it
 * can be safely carried through Flink checkpoint state and across operator boundaries without risk
 * of accidental mutation; build a new instance to derive a changed event.
 */
public final class ProcessedEvent implements Serializable {

  private final UUID id;
  private final String eventType;
  private final Instant eventTime;
  private final String source;
  private final Map<String, Object> payload;
  private final String imageUrl;
  private final String imageObjectKey;
  private final LocalDate date;

  public ProcessedEvent(UUID id, String eventType, Instant eventTime, String source,
      Map<String, Object> payload, String imageUrl, String imageObjectKey, LocalDate date) {
    this.id = id;
    this.eventType = eventType;
    this.eventTime = eventTime;
    this.source = source;
    this.payload = payload;
    this.imageUrl = imageUrl;
    this.imageObjectKey = imageObjectKey;
    this.date = date;
  }

  public UUID getId() { return id; }

  public String getEventType() { return eventType; }

  public Instant getEventTime() { return eventTime; }

  public String getSource() { return source; }

  public Map<String, Object> getPayload() { return payload; }

  public String getImageUrl() { return imageUrl; }

  public String getImageObjectKey() { return imageObjectKey; }

  public LocalDate getDate() { return date; }
}
