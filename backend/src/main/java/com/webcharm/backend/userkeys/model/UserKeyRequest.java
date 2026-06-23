package com.webcharm.backend.userkeys.model;

import com.webcharm.contract.userkeys.event.UserKeyFields;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Request body for a userKeys event: the identifying (userId, key) pair and a numeric value. */
public class UserKeyRequest {

  @NotBlank
  private String userId;

  @NotBlank
  private String key;

  @NotNull
  private Long value;

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Long getValue() {
    return value;
  }

  public void setValue(Long value) {
    this.value = value;
  }

  public Map<String, Object> toEventMap(UUID id, Instant eventTime, String source) {
    Map<String, Object> event = new HashMap<>();
    event.put(UserKeyFields.ID, id.toString());
    event.put(UserKeyFields.EVENT_TIME, eventTime.toString());
    event.put(UserKeyFields.SOURCE, source);
    event.put(UserKeyFields.USER_ID, userId);
    event.put(UserKeyFields.KEY, key);
    event.put(UserKeyFields.VALUE, value);
    return event;
  }
}
