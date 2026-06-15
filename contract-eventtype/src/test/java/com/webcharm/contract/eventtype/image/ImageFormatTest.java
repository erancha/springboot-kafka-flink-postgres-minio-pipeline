package com.webcharm.contract.eventtype.image;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ImageFormatTest {

  @Test
  void extensionForContentType_mapsKnownImageTypes() {
    assertEquals(".png", ImageFormat.extensionForContentType("image/png"));
    assertEquals(".webp", ImageFormat.extensionForContentType("image/webp"));
    assertEquals(".gif", ImageFormat.extensionForContentType("image/gif"));
  }

  @Test
  void extensionForContentType_fallsBackToJpgForJpegAndUnknownAndNull() {
    assertEquals(".jpg", ImageFormat.extensionForContentType("image/jpeg"));
    assertEquals(".jpg", ImageFormat.extensionForContentType("application/pdf"));
    assertEquals(".jpg", ImageFormat.extensionForContentType(null));
  }

  @Test
  void contentTypeForExtension_mapsKnownExtensions() {
    assertEquals("image/png", ImageFormat.contentTypeForExtension(".png"));
    assertEquals("image/webp", ImageFormat.contentTypeForExtension(".webp"));
    assertEquals("image/gif", ImageFormat.contentTypeForExtension(".gif"));
  }

  @Test
  void contentTypeForExtension_fallsBackToJpegForJpgAndUnknown() {
    assertEquals("image/jpeg", ImageFormat.contentTypeForExtension(".jpg"));
    assertEquals("image/jpeg", ImageFormat.contentTypeForExtension(".bmp"));
  }

  @Test
  void extensionForUrl_readsKnownSuffixFromPath() {
    assertEquals(".png", ImageFormat.extensionForUrl("https://cdn.example.com/a/b.png"));
    assertEquals(".webp", ImageFormat.extensionForUrl("https://cdn.example.com/x.webp?v=1"));
    assertEquals(".gif", ImageFormat.extensionForUrl("https://cdn.example.com/x.GIF"));
  }

  @Test
  void extensionForUrl_fallsBackToJpgForUnknownSuffixOrMalformedUrl() {
    assertEquals(".jpg", ImageFormat.extensionForUrl("https://cdn.example.com/photo.jpeg"));
    assertEquals(".jpg", ImageFormat.extensionForUrl("https://cdn.example.com/noext"));
    assertEquals(".jpg", ImageFormat.extensionForUrl("not a url"));
  }
}
