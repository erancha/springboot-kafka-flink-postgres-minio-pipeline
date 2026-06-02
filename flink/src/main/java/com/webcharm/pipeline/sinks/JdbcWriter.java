package com.webcharm.pipeline.sinks;

import java.io.IOException;

/**
 * Minimal contract for a JDBC writer: write one record and release resources when done.
 * Intentionally narrow — implementations are driven by ProcessFunctions, not by the Flink Sink API.
 */
public interface JdbcWriter<T> extends AutoCloseable {
  void write(T value) throws IOException;
}
