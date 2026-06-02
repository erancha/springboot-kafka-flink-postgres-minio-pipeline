package com.webcharm.pipeline.config;

/** Environment-variable lookup with defaults. */
public final class EnvConfig {
  private EnvConfig() {}

  /** Reads a string env var; falls back to defaultValue when unset or blank. */
  public static String env(String name, String defaultValue) {
    String v = System.getenv(name);
    return (v == null || v.isBlank()) ? defaultValue : v;
  }

  /** Reads an int env var; falls back to defaultValue when unset, blank, or unparseable. */
  public static int envInt(String name, int defaultValue) {
    String v = System.getenv(name);
    if (v == null || v.isBlank()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(v.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
