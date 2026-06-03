package com.webcharm.backend.kafka;

/** Thrown when the Kafka send future completes with a failure or times out; allows callers to distinguish broker errors from other runtime failures. */
public class KafkaPublishException extends RuntimeException {
  public KafkaPublishException(String message, Throwable cause) {
    super(message, cause);
  }
}
