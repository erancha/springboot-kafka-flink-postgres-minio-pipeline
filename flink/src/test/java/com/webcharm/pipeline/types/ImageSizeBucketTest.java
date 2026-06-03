package com.webcharm.pipeline.types;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Pins the size-bucket boundaries: each is inclusive of its upper bound, with >10MB as overflow. */
class ImageSizeBucketTest {

  private static final long MB = 1024L * 1024L;

  @Test
  void boundariesAreInclusiveOfUpperBound() {
    assertEquals(ImageSizeBucket.UP_TO_1MB, ImageSizeBucket.of(0));
    assertEquals(ImageSizeBucket.UP_TO_1MB, ImageSizeBucket.of(MB));
    assertEquals(ImageSizeBucket.UP_TO_5MB, ImageSizeBucket.of(MB + 1));
    assertEquals(ImageSizeBucket.UP_TO_5MB, ImageSizeBucket.of(5 * MB));
    assertEquals(ImageSizeBucket.UP_TO_10MB, ImageSizeBucket.of(5 * MB + 1));
    assertEquals(ImageSizeBucket.UP_TO_10MB, ImageSizeBucket.of(10 * MB));
    assertEquals(ImageSizeBucket.OVER_10MB, ImageSizeBucket.of(10 * MB + 1));
  }
}
