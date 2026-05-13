package com.webcharm.backend.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class EventRequest {
  @NotBlank
  private String eventType;

  private Map<String, Object> payload;

  private String imageUrl;

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public Map<String, Object> getPayload() {
    return payload;
  }

  public void setPayload(Map<String, Object> payload) {
    this.payload = payload;
  }

  public String getImageUrl() {
    return imageUrl;
  }

  public void setImageUrl(String imageUrl) {
    this.imageUrl = imageUrl;
  }

  public Map<String, Object> toEventMap(@NotNull UUID id, @NotNull Instant eventTime, @NotBlank String source) {
    Map<String, Object> event = new HashMap<>();
    event.put("id", id.toString());
    event.put("eventType", eventType);
    event.put("eventTime", eventTime.toString());
    event.put("source", source);

    if (payload != null) {
      event.put("payload", payload);
    }

    if (imageUrl != null && !imageUrl.isBlank()) {
      event.put("imageUrl", imageUrl);
    }

    return event;
  }
}
