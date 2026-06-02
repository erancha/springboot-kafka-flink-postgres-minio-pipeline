package com.webcharm.backend.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that the producer serializes events to JSON, uses the event id as the Kafka partition key,
 * and throws KafkaPublishException on send failure so the caller receives a 503 rather than a false 200.
 * KafkaTemplate is replaced by a Mockito mock; no real Kafka broker is needed.
 */
@ExtendWith(MockitoExtension.class)
class EventProducerTest {

  @Mock
  KafkaTemplate<String, String> kafkaTemplate;
  @Mock
  SendResult<String, String> sendResult;
  @Mock
  RecordMetadata recordMetadata;

  EventProducer producer;

  @BeforeEach
  void setUp() {
    producer = new EventProducer(kafkaTemplate, new ObjectMapper(), "events", 5);
  }

  @Test
  void send_usesEventIdAsKeyAndSerializesToJson() {
    when(sendResult.getRecordMetadata()).thenReturn(recordMetadata);
    when(kafkaTemplate.send(eq("events"), eq("abc-123"), argThat(json -> json.contains("\"DATA\""))))
        .thenReturn(CompletableFuture.completedFuture(sendResult));
    Map<String, Object> event = Map.of("id", "abc-123", "eventType", "DATA");

    producer.send(event);

    verify(kafkaTemplate).send(
        eq("events"),
        eq("abc-123"),
        argThat(json -> json.contains("\"DATA\"") && json.contains("\"abc-123\"")));
  }

  @Test
  void send_missingId_usesEmptyStringKey() {
    when(sendResult.getRecordMetadata()).thenReturn(recordMetadata);
    when(kafkaTemplate.send(eq("events"), eq(""), argThat(json -> json.contains("\"DATA\""))))
        .thenReturn(CompletableFuture.completedFuture(sendResult));
    Map<String, Object> event = Map.of("eventType", "DATA");

    producer.send(event);

    verify(kafkaTemplate).send(eq("events"), eq(""), argThat(json -> json.contains("\"DATA\"")));
  }

  @Test
  void send_kafkaFailure_throwsKafkaPublishException() {
    CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("broker unavailable"));
    when(kafkaTemplate.send(eq("events"), eq("err-id"), argThat(s -> s.contains("err-id"))))
        .thenReturn(failed);

    Map<String, Object> event = Map.of("id", "err-id", "eventType", "DATA");

    assertThrows(KafkaPublishException.class, () -> producer.send(event));
  }
}
