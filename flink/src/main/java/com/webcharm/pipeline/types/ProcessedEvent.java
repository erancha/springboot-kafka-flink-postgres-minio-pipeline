package com.webcharm.pipeline.types;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

/** Event after Kafka deserialization. Carries only the fields written to Postgres. */
public class ProcessedEvent implements Serializable {

  private UUID id;
  private String eventType;
  private Instant eventTime;
  private String source;
  private Map<String, Object> payload;
  private String imageUrl;
  private String imageObjectKey;
  private LocalDate date;

  public ProcessedEvent() {}

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
  public void setId(UUID id) { this.id = id; }

  public String getEventType() { return eventType; }
  public void setEventType(String eventType) { this.eventType = eventType; }

  public Instant getEventTime() { return eventTime; }
  public void setEventTime(Instant eventTime) { this.eventTime = eventTime; }

  public String getSource() { return source; }
  public void setSource(String source) { this.source = source; }

  public Map<String, Object> getPayload() { return payload; }
  public void setPayload(Map<String, Object> payload) { this.payload = payload; }

  public String getImageUrl() { return imageUrl; }
  public void setImageUrl(String imageUrl) { this.imageUrl = imageUrl; }

  public String getImageObjectKey() { return imageObjectKey; }
  public void setImageObjectKey(String imageObjectKey) { this.imageObjectKey = imageObjectKey; }

  public LocalDate getDate() { return date; }
  public void setDate(LocalDate date) { this.date = date; }
}
