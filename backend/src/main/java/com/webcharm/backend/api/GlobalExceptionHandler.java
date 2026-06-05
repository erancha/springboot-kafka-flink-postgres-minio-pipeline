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

  // Status range (4xx vs 5xx) drives both log level and response body, so handlers inherit the
  // policy by passing their status. 4xx is caller-driven, so it logs at DEBUG (WARN would let an
  // attacker flood the log) and returns its client-actionable message. 5xx means a backing service
  // is failing, so it logs the detail at WARN but returns only the reason phrase — internal failure
  // detail must not be echoed to the caller.
  private ResponseEntity<String> respond(HttpStatus status, String body, Throwable ex) {
    String internalMessage = ex.getMessage();
    if (status.is4xxClientError()) {
      log.debug("{} -> {}: {}", ex.getClass().getSimpleName(), status.value(), internalMessage);
      return ResponseEntity.status(status).body(body);
    }
    log.warn("{} -> {}: {}", ex.getClass().getSimpleName(), status.value(), internalMessage);
    return ResponseEntity.status(status).body(status.getReasonPhrase());
  }
}
