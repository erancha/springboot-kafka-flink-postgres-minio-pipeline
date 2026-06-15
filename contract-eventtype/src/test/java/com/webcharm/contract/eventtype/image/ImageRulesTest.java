package com.webcharm.contract.eventtype.image;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ImageRulesTest {

  @Test
  void imageContentTypesAreAccepted() {
    assertTrue(ImageRules.isImageContentType("image/png"));
    assertTrue(ImageRules.isImageContentType("IMAGE/JPEG"));
  }

  @Test
  void nonImageOrAbsentContentTypesAreRejected() {
    assertFalse(ImageRules.isImageContentType("application/pdf"));
    assertFalse(ImageRules.isImageContentType(""));
    assertFalse(ImageRules.isImageContentType(null));
  }
}
