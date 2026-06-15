package com.webcharm.contract.eventtype.image;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ImageObjectKeyTest {

  private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000001");

  @Test
  void of_buildsImagesDateIdExtensionLayout() {
    assertEquals(
        "images/2026-05-17/00000000-0000-0000-0000-000000000001.png",
        ImageObjectKey.of(LocalDate.of(2026, 5, 17), ID, ".png"));
  }

  @Test
  void of_usesIsoDateSegment() {
    assertEquals(
        "images/2026-01-02/00000000-0000-0000-0000-000000000001.jpg",
        ImageObjectKey.of(LocalDate.of(2026, 1, 2), ID, ".jpg"));
  }
}
