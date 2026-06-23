package com.webcharm.backend.userkeys.model;

/** Response for an accepted userKeys event: the assigned id and the timestamp stamped at ingestion. */
public class UserKeyResponse {
  private final String id;
  private final String eventTime;

  public UserKeyResponse(String id, String eventTime) {
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
