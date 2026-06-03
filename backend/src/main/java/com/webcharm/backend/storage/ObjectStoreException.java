package com.webcharm.backend.storage;

/** Signals an object store upload failure: the image bytes were not durably stored. */
public class ObjectStoreException extends RuntimeException {
  public ObjectStoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
