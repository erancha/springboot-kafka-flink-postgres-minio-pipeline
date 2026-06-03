package com.webcharm.pipeline.sinks;

import java.io.IOException;

/**
 * Signals a permanent JDBC failure (constraint violation, invalid JSONB, etc.) that retry cannot fix.
 * Extends IOException so callers that only distinguish IOException from non-IOException still compile,
 * but callers that need to route permanent failures differently (e.g. to a DLQ) can catch it first.
 */
public class PermanentJdbcException extends IOException {
  public PermanentJdbcException(String message, Throwable cause) {
    super(message, cause);
  }
}
