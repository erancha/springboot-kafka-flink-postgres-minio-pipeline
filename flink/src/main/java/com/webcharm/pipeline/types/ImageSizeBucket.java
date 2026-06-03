package com.webcharm.pipeline.types;

/**
 * Stored-image size histogram bucket; of(bytes) holds the only definition of the boundaries.
 *
 * OVER_10MB is unreachable under the current 10 MB ingress cap enforced on both upload paths
 * (backend multipart and Flink URL fetch); it is a forward-compatible overflow bucket that only
 * receives images if that cap is ever raised.
 */
public enum ImageSizeBucket {
  UP_TO_1MB("<=1MB"),
  UP_TO_5MB("<=5MB"),
  UP_TO_10MB("<=10MB"),
  OVER_10MB(">10MB");

  private static final long MB = 1024L * 1024L;

  private final String label;

  ImageSizeBucket(String label) {
    this.label = label;
  }

  public String label() {
    return label;
  }

  public static ImageSizeBucket of(long bytes) {
    if (bytes <= MB) {
      return UP_TO_1MB;
    }
    if (bytes <= 5 * MB) {
      return UP_TO_5MB;
    }
    if (bytes <= 10 * MB) {
      return UP_TO_10MB;
    }
    return OVER_10MB;
  }
}
