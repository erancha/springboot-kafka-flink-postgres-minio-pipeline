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
  private final int sendTimeoutSeconds;

  public EventProducer(
      KafkaTemplate<String, String> kafkaTemplate,
      ObjectMapper objectMapper,
      @Value("${app.kafka.send-timeout-seconds:5}") int sendTimeoutSeconds) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
    this.sendTimeoutSeconds = sendTimeoutSeconds;
  }

  /**
   * Serializes the event to JSON and publishes it to the given topic under the given partition key,
   * blocking until the broker acknowledges or the send-timeout elapses. Throws KafkaPublishException
   * on broker failure, timeout, or thread interruption, so the caller receives a 503 instead of a
   * false 200.
   */
  public void send(String topic, String key, Map<String, Object> event) {
    String value;
    try {
      value = objectMapper.writeValueAsString(event);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize event to JSON", e);
    }

    try {
      var result = kafkaTemplate.send(topic, key, value).get(sendTimeoutSeconds, TimeUnit.SECONDS);
      log.debug("Published event id={} to topic={} partition={} offset={}",
          key, topic,
          result.getRecordMetadata().partition(),
          result.getRecordMetadata().offset());
    } catch (TimeoutException e) {
      throw new KafkaPublishException(
          "Kafka send timed out for event id=" + key + " to topic=" + topic + " after " + sendTimeoutSeconds + "s", e);
    } catch (ExecutionException e) {
      throw new KafkaPublishException(
          "Kafka send failed for event id=" + key + " to topic=" + topic + ": " + e.getCause().getMessage(),
          e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new KafkaPublishException("Interrupted while publishing event id=" + key, e);
    }
  }
}
