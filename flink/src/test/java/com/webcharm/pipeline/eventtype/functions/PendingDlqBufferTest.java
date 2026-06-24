package com.webcharm.pipeline.eventtype.functions;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.webcharm.pipeline.common.dlq.DlqRecord;
import com.webcharm.pipeline.eventtype.types.DlqStage;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies the dead-letter buffer that holds records found during a checkpoint flush: it drains for
 * emission and round-trips through JSON so the records survive a restart via Flink list state.
 */
class PendingDlqBufferTest {

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

  private static DlqRecord rec(String raw) {
    return new DlqRecord(DlqStage.DATA_POSTGRES.name(), raw, "constraint", Instant.parse("2026-05-18T00:00:00Z"));
  }

  @Test
  void drain_returnsBufferedRecordsAndClears() {
    PendingDlqBuffer buffer = new PendingDlqBuffer();
    buffer.add(rec("a"));
    buffer.add(rec("b"));

    List<DlqRecord> drained = buffer.drain();

    assertEquals(List.of(rec("a"), rec("b")), drained);
    assertTrue(buffer.isEmpty());
    assertTrue(buffer.drain().isEmpty());
  }

  @Test
  void toJson_thenRestoreFrom_roundTrips() throws Exception {
    PendingDlqBuffer source = new PendingDlqBuffer();
    source.add(rec("a"));
    source.add(rec("b"));

    List<String> persisted = source.toJson(mapper);

    PendingDlqBuffer restored = new PendingDlqBuffer();
    restored.restoreFrom(persisted, mapper);

    assertEquals(List.of(rec("a"), rec("b")), restored.drain());
  }
}
