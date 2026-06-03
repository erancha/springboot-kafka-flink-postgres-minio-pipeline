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
  // Stored-image byte size feeding the size-bucket histogram; null when the size was not
  // determined. Only ever populated on a success result.
  private final Long imageBytes;

  private EnrichResult(ProcessedEvent success, DlqRecord failure, boolean retryable, Long imageBytes) {
    this.success = success;
    this.failure = failure;
    this.retryable = retryable;
    this.imageBytes = imageBytes;
  }

  public static EnrichResult success(ProcessedEvent event) {
    return new EnrichResult(event, null, false, null);
  }

  public static EnrichResult success(ProcessedEvent event, Long imageBytes) {
    return new EnrichResult(event, null, false, imageBytes);
  }

  public static EnrichResult permanentFailure(DlqRecord record) {
    return new EnrichResult(null, record, false, null);
  }

  public static EnrichResult retryableFailure(DlqRecord record) {
    return new EnrichResult(null, record, true, null);
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

  public Long imageBytes() {
    return imageBytes;
  }

  public DlqRecord failure() {
    return failure;
  }
}
