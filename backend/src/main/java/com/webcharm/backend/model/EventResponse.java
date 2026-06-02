package com.webcharm.backend.model;

public class EventResponse {
  private final String id;
  private final String eventTime;

  public EventResponse(String id, String eventTime) {
    this.id = id;
    this.eventTime = eventTime;
  }

  public String getId() {
    return id;
  }

  public String getEventTime() {
    return eventTime;
  }
}
