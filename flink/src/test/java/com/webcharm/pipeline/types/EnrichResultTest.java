package com.webcharm.pipeline.types;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Verifies the EnrichResult factories and accessors: success, permanent failure, retryable failure. */
class EnrichResultTest {

  private static ProcessedEvent event() {
    return new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, "images/k.jpg", LocalDate.now());
  }

  private static DlqRecord dlq() {
    return new DlqRecord(DlqStage.IMAGE_ENRICH, "raw", "boom", Instant.now());
  }

  @Test
  void success_isSuccess_notRetryable_carriesEvent() {
    ProcessedEvent e = event();
    EnrichResult r = EnrichResult.success(e);
    assertTrue(r.isSuccess());
    assertFalse(r.isRetryable());
    assertSame(e, r.success());
    assertNull(r.failure());
  }

  @Test
  void permanentFailure_notSuccess_notRetryable_carriesDlq() {
    DlqRecord d = dlq();
    EnrichResult r = EnrichResult.permanentFailure(d);
    assertFalse(r.isSuccess());
    assertFalse(r.isRetryable());
    assertSame(d, r.failure());
    assertNull(r.success());
  }

  @Test
  void retryableFailure_notSuccess_isRetryable_carriesDlq() {
    DlqRecord d = dlq();
    EnrichResult r = EnrichResult.retryableFailure(d);
    assertFalse(r.isSuccess());
    assertTrue(r.isRetryable());
    assertSame(d, r.failure());
  }
}
