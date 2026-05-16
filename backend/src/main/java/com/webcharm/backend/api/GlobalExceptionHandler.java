package com.webcharm.backend.api;

import com.webcharm.backend.kafka.KafkaPublishException;
import com.webcharm.backend.storage.ObjectStoreException;
import java.io.IOException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Maps application exceptions to HTTP responses so controllers stay free of status-code logic. */
@RestControllerAdvice
public class GlobalExceptionHandler {

  /** Returns 400 for rejected input (e.g. empty upload file). */
  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<String> handleIllegalArgument(IllegalArgumentException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ex.getMessage());
  }

  /** Returns 500 when reading the uploaded file bytes fails at the OS/stream level. */
  @ExceptionHandler(IOException.class)
  public ResponseEntity<String> handleIoException(IOException ex) {
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
  }

  /** Returns 503 when the object store upload fails, signalling the image was not stored. */
  @ExceptionHandler(ObjectStoreException.class)
  public ResponseEntity<String> handleObjectStoreException(ObjectStoreException ex) {
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ex.getMessage());
  }

  /** Returns 503 when the Kafka broker rejects or times out a send, signalling the event was not enqueued. */
  @ExceptionHandler(KafkaPublishException.class)
  public ResponseEntity<String> handleKafkaPublishException(KafkaPublishException ex) {
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ex.getMessage());
  }
}
