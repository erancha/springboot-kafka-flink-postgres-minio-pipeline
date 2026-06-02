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

  public static EnrichResult success(ProcessedEvent event) {
    return new EnrichResult(event, null, false);
  }

  public static EnrichResult permanentFailure(DlqRecord record) {
    return new EnrichResult(null, record, false);
  }

  public static EnrichResult retryableFailure(DlqRecord record) {
    return new EnrichResult(null, record, true);
  }

  public boolean isSuccess() {
    return success != null;
  }

  public boolean isRetryable() {
    return failure != null && retryable;
  }

  public ProcessedEvent success() {
    return success;
  }

  public DlqRecord failure() {
    return failure;
  }
}
