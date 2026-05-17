package com.webcharm.pipeline.types;

import java.io.Serializable;

/**
 * Immutable outcome of image enrichment: either an enriched ProcessedEvent (success) or a
 * DlqRecord plus a retryable flag (failure). Exactly one of the two is present.
 */
public final class EnrichResult implements Serializable {

  private final ProcessedEvent success;
  private final DlqRecord failure;
  private final boolean retryable;

  private EnrichResult(ProcessedEvent success, DlqRecord failure, boolean retryable) {
    this.success = success;
    this.failure = failure;
    this.retryable = retryable;
  }

  /** Creates a success result holding the enriched event. */
  public static EnrichResult success(ProcessedEvent event) {
    return new EnrichResult(event, null, false);
  }

  /** Creates a non-retryable failure result for an unrecoverable error. */
  public static EnrichResult permanentFailure(DlqRecord record) {
    return new EnrichResult(null, record, false);
  }

  /** Creates a retryable failure result for a transient error. */
  public static EnrichResult retryableFailure(DlqRecord record) {
    return new EnrichResult(null, record, true);
  }

  /** True when this carries an enriched event rather than a failure. */
  public boolean isSuccess() {
    return success != null;
  }

  /** True when this is a failure marked retryable (transient). */
  public boolean isRetryable() {
    return failure != null && retryable;
  }

  /** The enriched event, or null when this is a failure. */
  public ProcessedEvent success() {
    return success;
  }

  /** The DLQ record, or null when this is a success. */
  public DlqRecord failure() {
    return failure;
  }
}
