package com.webcharm.pipeline.common.config;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Verifies EnvConfig.envInt falls back to the default for unset and unparseable values. */
class EnvConfigTest {

  @Test
  void envInt_unsetName_returnsDefault() {
    assertEquals(42, EnvConfig.envInt("DEFINITELY_UNSET_VAR_XYZ", 42));
  }

  @Test
  void envInt_unparseableValue_returnsDefault() {
    // PATH is virtually always set and is not an integer.
    assertEquals(7, EnvConfig.envInt("PATH", 7));
  }
}
