package com.webcharm.backend.storage;

/** Signals an object store upload failure; mapped to 503 by GlobalExceptionHandler. */
public class ObjectStoreException extends RuntimeException {
  public ObjectStoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
