package com.webcharm.contract.eventtype.image;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * The images/{date}/{id}{extension} object-key layout for stored images. Every path that stores an
 * image derives its key here, so one image cannot land under two divergent keys.
 */
public final class ImageObjectKey {

  private static final String PREFIX = "images/";

  /** Builds the object key for an image with the given event date, id, and file extension. */
  public static String of(LocalDate date, UUID id, String extension) {
    return PREFIX + DateTimeFormatter.ISO_LOCAL_DATE.format(date) + "/" + id + extension;
  }

  private ImageObjectKey() {}
}
