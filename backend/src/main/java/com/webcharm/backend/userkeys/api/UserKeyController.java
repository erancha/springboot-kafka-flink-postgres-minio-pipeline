package com.webcharm.backend.userkeys.api;

import com.webcharm.backend.kafka.EventProducer;
import com.webcharm.backend.userkeys.model.UserKeyRequest;
import com.webcharm.backend.userkeys.model.UserKeyResponse;
import com.webcharm.backend.util.UuidV7;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** REST controller that validates and publishes userKeys events to Kafka. */
@RestController
@RequestMapping("/api")
public class UserKeyController {

  @Value("${app.userkeys.kafka.topic}")
  private String topic;

  private final EventProducer eventProducer;

  public UserKeyController(EventProducer eventProducer) {
    this.eventProducer = eventProducer;
  }

  /** Validates and publishes a userKeys event to Kafka; returns the assigned id and timestamp. */
  @PostMapping("/user-keys")
  public UserKeyResponse publishUserKey(@Valid @RequestBody UserKeyRequest request) {
    UUID id = UuidV7.next();
    Instant now = Instant.now();
    Map<String, Object> event = request.toEventMap(id, now, "ui");
    // Kafka partition key co-locates one (userId, key) pair's events on a single partition, keeping
    // their publish order intact. The '|' delimiter prevents pairs like ("ab","c") and ("a","bc")
    // from colliding onto the same key.
    String partitionKey = request.getUserId() + "|" + request.getKey();
    eventProducer.send(topic, partitionKey, event);
    return new UserKeyResponse(id.toString(), now.toString());
  }
}
