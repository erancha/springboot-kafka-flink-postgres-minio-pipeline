package com.webcharm.backend.model;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class EventRequest {
  @NotNull
  private EventType eventType;

  private Map<String, Object> payload;

  private String imageUrl;

  public EventType getEventType() {
    return eventType;
  }

  public void setEventType(EventType eventType) {
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

  /**
   * Requires a non-empty payload for DATA and its TEXT alias. IMAGE is exempt: it has no payload —
   * its content is an imageUrl or an uploaded object. A null eventType is left to {@code @NotNull}.
   */
  @AssertTrue(message = "DATA events require a non-empty payload")
  public boolean isPayloadPresentWhenRequired() {
    if (eventType == null || eventType == EventType.IMAGE) {
      return true;
    }
    return payload != null && !payload.isEmpty();
  }

  public Map<String, Object> toEventMap(@NotNull UUID id, @NotNull Instant eventTime, @NotNull String source) {
    Map<String, Object> event = new HashMap<>();
    event.put("id", id.toString());
    event.put("eventType", eventType.normalized());
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
