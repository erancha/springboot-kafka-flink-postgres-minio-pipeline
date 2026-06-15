package com.webcharm.contract.eventtype.event;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class EventTypesTest {

  @Test
  void canonicalValuesAreRecognized() {
    assertTrue(EventTypes.isCanonical(EventTypes.DATA));
    assertTrue(EventTypes.isCanonical(EventTypes.IMAGE));
  }

  @Test
  void nonCanonicalValuesAreRejected() {
    assertFalse(EventTypes.isCanonical("TEXT"));
    assertFalse(EventTypes.isCanonical("UNEXPECTED"));
    assertFalse(EventTypes.isCanonical(null));
  }
}
