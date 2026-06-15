package com.webcharm.contract.eventtype.image;

import java.util.Locale;

/**
 * Constraints on what counts as a storable image. The backend applies them to multipart uploads and
 * the Flink job applies them to bytes fetched from an imageUrl, so both sides enforce one definition
 * of an acceptable image instead of two copies that can diverge.
 */
public final class ImageRules {

  /** Upper bound on stored image size; larger payloads are rejected rather than stored. */
  public static final long MAX_IMAGE_BYTES = 10L * 1024 * 1024;

  /** Whether a Content-Type denotes an image. A null or non-image type is rejected. */
  public static boolean isImageContentType(String contentType) {
    return contentType != null && contentType.toLowerCase(Locale.ROOT).startsWith("image/");
  }

  private ImageRules() {}
}
