package com.webcharm.backend.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventProducer {

  private static final Logger log = LoggerFactory.getLogger(EventProducer.class);

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;
  private final String topic;
  private final int sendTimeoutSeconds;

  /** Spring constructor — all dependencies injected; sendTimeoutSeconds defaults to 5 if app.kafka.send-timeout-seconds is not set. */
  public EventProducer(
      KafkaTemplate<String, String> kafkaTemplate,
      ObjectMapper objectMapper,
      @Value("${app.kafka.topic}") String topic,
      @Value("${app.kafka.send-timeout-seconds:5}") int sendTimeoutSeconds) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
    this.topic = topic;
    this.sendTimeoutSeconds = sendTimeoutSeconds;
  }

  /**
   * Serializes the event to JSON and publishes it to Kafka, blocking until the broker acknowledges
   * or the send-timeout elapses. Throws KafkaPublishException on broker failure, timeout, or
   * thread interruption, so the caller receives a 503 instead of a false 200.
   */
  public void send(Map<String, Object> event) {
    String key = String.valueOf(event.getOrDefault("id", ""));

    String value;
    try {
      value = objectMapper.writeValueAsString(event);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize event to JSON", e);
    }

    log.debug("Publishing event id={} to topic={}", key, topic);
    try {
      var result = kafkaTemplate.send(topic, key, value).get(sendTimeoutSeconds, TimeUnit.SECONDS);
      log.debug("Published event id={} to topic={} partition={} offset={}",
          key, topic,
          result.getRecordMetadata().partition(),
          result.getRecordMetadata().offset());
    } catch (TimeoutException e) {
      log.error("Timed out publishing event id={} to topic={} after {}s", key, topic, sendTimeoutSeconds);
      throw new KafkaPublishException("Kafka send timed out for event id=" + key, e);
    } catch (ExecutionException e) {
      log.error("Failed to publish event id={} to topic={}: {}", key, topic, e.getCause().getMessage());
      throw new KafkaPublishException("Kafka send failed for event id=" + key, e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new KafkaPublishException("Interrupted while publishing event id=" + key, e);
    }
  }
}
