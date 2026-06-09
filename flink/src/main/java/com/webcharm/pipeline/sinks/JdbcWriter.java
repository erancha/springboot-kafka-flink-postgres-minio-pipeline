package com.webcharm.pipeline.sinks;

import java.io.IOException;
import java.util.List;

/**
 * Minimal contract for a buffered JDBC writer: buffer rows, flush them as one committed batch, and
 * release resources when done. Intentionally narrow — implementations are driven by ProcessFunctions,
 * not by the Flink Sink API.
 *
 * write() and flush() return the rows that failed permanently (constraint violation, invalid JSONB)
 * so the caller can route them to a dead-letter sink; transient failures throw IOException so Flink
 * replays the checkpoint instead.
 */
public interface JdbcWriter<T> extends AutoCloseable {

  /** A row that a flush could not write for a non-retryable reason, paired with the failure reason. */
  record FailedRow<T>(T value, String reason) {}

  /** Buffers one record; may flush when the batch fills. Returns rows that permanently failed during
      a flush triggered by this call (empty on the happy path). */
  List<FailedRow<T>> write(T value) throws IOException;

  /** Force-flushes buffered records. Returns rows that permanently failed; transient failures throw. */
  List<FailedRow<T>> flush() throws IOException;
}
