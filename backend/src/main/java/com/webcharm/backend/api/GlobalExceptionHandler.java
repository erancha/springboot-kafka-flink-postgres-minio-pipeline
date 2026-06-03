package com.webcharm.backend.api;

import com.webcharm.backend.kafka.KafkaPublishException;
import com.webcharm.backend.storage.ObjectStoreException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Maps application exceptions to HTTP responses so controllers stay free of status-code logic. */
@RestControllerAdvice
public class GlobalExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

  /** Returns 400 for rejected input (e.g. empty upload file, malformed URL). */
  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<String> handleIllegalArgument(IllegalArgumentException ex) {
    return respond(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
  }

  /** Returns 400 when the request body cannot be parsed (e.g. unknown eventType enum value). */
  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<String> handleNotReadable(HttpMessageNotReadableException ex) {
    return respond(HttpStatus.BAD_REQUEST, "Invalid request body: " + ex.getMostSpecificCause().getMessage(), ex);
  }

  /** Returns 403 when an imageUrl is rejected by the SSRF allowlist. */
  @ExceptionHandler(SecurityException.class)
  public ResponseEntity<String> handleSecurityException(SecurityException ex) {
    return respond(HttpStatus.FORBIDDEN, ex.getMessage(), ex);
  }

  /** Returns 500 when reading the uploaded file bytes fails at the OS/stream level. */
  @ExceptionHandler(IOException.class)
  public ResponseEntity<String> handleIoException(IOException ex) {
    return respond(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage(), ex);
  }

  /** Returns 503 when the object store upload fails, signalling the image was not stored. */
  @ExceptionHandler(ObjectStoreException.class)
  public ResponseEntity<String> handleObjectStoreException(ObjectStoreException ex) {
    return respond(HttpStatus.SERVICE_UNAVAILABLE, ex.getMessage(), ex);
  }

  /** Returns 503 when the Kafka broker rejects or times out a send, signalling the event was not enqueued. */
  @ExceptionHandler(KafkaPublishException.class)
  public ResponseEntity<String> handleKafkaPublishException(KafkaPublishException ex) {
    return respond(HttpStatus.SERVICE_UNAVAILABLE, ex.getMessage(), ex);
  }

  // Log level is chosen from the HTTP status range (4xx vs 5xx), not the exception type, so any
  // handler added later inherits the policy by passing its status:
  // - 4xx (client/attacker-driven input, including SSRF denials): DEBUG. The caller controls the
  //   rate, so logging every one at WARN is a flooding vector; available when investigating,
  //   silent under load.
  // - 5xx (dependency or IO failure): WARN. Signals a backing service is actually failing and
  //   must stay visible in production.
  private ResponseEntity<String> respond(HttpStatus status, String body, Throwable ex) {
    String detail = ex.getMessage();
    if (status.is4xxClientError()) {
      log.debug("{} -> {}: {}", ex.getClass().getSimpleName(), status.value(), detail);
    } else {
      log.warn("{} -> {}: {}", ex.getClass().getSimpleName(), status.value(), detail);
    }
    return ResponseEntity.status(status).body(body);
  }
}
