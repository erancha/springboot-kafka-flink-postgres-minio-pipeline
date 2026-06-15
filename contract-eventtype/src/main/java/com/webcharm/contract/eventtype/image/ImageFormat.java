package com.webcharm.contract.eventtype.image;

import java.net.URI;
import java.util.Locale;

/**
 * The image formats the pipeline stores, pairing each file extension with its Content-Type. One
 * mapping backs extension-from-Content-Type, Content-Type-from-extension, and extension-from-URL, so
 * those derivations stay mutually consistent. Anything that is not a recognized non-JPEG format
 * resolves to JPEG, the default.
 */
public enum ImageFormat {

  PNG(".png", "image/png"),
  WEBP(".webp", "image/webp"),
  GIF(".gif", "image/gif"),
  JPEG(".jpg", "image/jpeg");

  private final String extension;
  private final String contentType;

  ImageFormat(String extension, String contentType) {
    this.extension = extension;
    this.contentType = contentType;
  }

  /** Extension implied by a Content-Type, matched loosely on the type token; null or unrecognized yields .jpg. */
  public static String extensionForContentType(String contentType) {
    String ct = contentType == null ? "" : contentType.toLowerCase(Locale.ROOT);
    for (ImageFormat format : values()) {
      if (format != JPEG && ct.contains(format.token())) {
        return format.extension;
      }
    }
    return JPEG.extension;
  }

  /** Content-Type for a file extension; an unrecognized extension yields image/jpeg. */
  public static String contentTypeForExtension(String extension) {
    for (ImageFormat format : values()) {
      if (format.extension.equals(extension)) {
        return format.contentType;
      }
    }
    return JPEG.contentType;
  }

  /** Extension read from a URL's path suffix; a missing suffix or a malformed URL yields .jpg. */
  public static String extensionForUrl(String url) {
    try {
      String path = URI.create(url).getPath().toLowerCase(Locale.ROOT);
      for (ImageFormat format : values()) {
        if (format != JPEG && path.endsWith(format.extension)) {
          return format.extension;
        }
      }
    } catch (RuntimeException ignored) {
      // A malformed URL cannot yield an extension; fall through to the default.
    }
    return JPEG.extension;
  }

  /** The extension without its leading dot, used for loose Content-Type matching (e.g. "png"). */
  private String token() {
    return extension.substring(1);
  }
}
