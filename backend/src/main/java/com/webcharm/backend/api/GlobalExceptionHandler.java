package com.webcharm.backend.api;

import com.webcharm.backend.kafka.KafkaPublishException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Maps application exceptions to HTTP responses so controllers stay free of status-code logic. */
@RestControllerAdvice
public class GlobalExceptionHandler {

  /** Returns 503 when the Kafka broker rejects or times out a send, signalling the event was not enqueued. */
  @ExceptionHandler(KafkaPublishException.class)
  public ResponseEntity<String> handleKafkaPublishException(KafkaPublishException ex) {
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ex.getMessage());
  }
}
