package com.webcharm.pipeline.config;

/** Single place for env-var lookup with defaults; avoids the same 3-line pattern across every class. */
public final class EnvConfig {
  private EnvConfig() {}

  public static String env(String name, String defaultValue) {
    String v = System.getenv(name);
    return (v == null || v.isBlank()) ? defaultValue : v;
  }
}
