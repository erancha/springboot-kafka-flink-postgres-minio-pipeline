package com.webcharm.pipeline.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Verifies DlqRecord serializes its stage to the enum name and round-trips, matching what
 * StreamingJob.DlqRecordSerializer does (plain ObjectMapper + JavaTimeModule, no custom enum config).
 */
class DlqRecordJsonTest {

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

  @Test
  void serializesStageAsEnumName() throws Exception {
    DlqRecord r = new DlqRecord(DlqStage.PARSE, "raw-json", "boom", Instant.parse("2026-05-18T00:00:00Z"));
    String json = mapper.writeValueAsString(r);
    assertTrue(json.contains("\"stage\":\"PARSE\""), json);
  }

  @Test
  void roundTripsAllFields() throws Exception {
    DlqRecord r = new DlqRecord(DlqStage.IMAGE_POSTGRES, "raw", "err", Instant.parse("2026-05-18T00:00:00Z"));
    DlqRecord back = mapper.readValue(mapper.writeValueAsString(r), DlqRecord.class);
    assertEquals(r, back);
  }
}
