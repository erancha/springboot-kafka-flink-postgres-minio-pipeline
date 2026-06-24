package com.webcharm.pipeline.common.dlq;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Verifies DlqRecord serializes its stage as a plain string and round-trips, matching what
 * DlqRecordSerializer does (plain ObjectMapper + JavaTimeModule). A pipeline's DlqStage enum
 * serializes to its constant name, so this string form is identical to a typed-enum field.
 */
class DlqRecordJsonTest {

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

  @Test
  void serializesStageAsString() throws Exception {
    DlqRecord r = new DlqRecord("PARSE", "raw-json", "boom", Instant.parse("2026-05-18T00:00:00Z"));
    String json = mapper.writeValueAsString(r);
    assertTrue(json.contains("\"stage\":\"PARSE\""), json);
  }

  @Test
  void roundTripsAllFields() throws Exception {
    DlqRecord r = new DlqRecord("IMAGE_POSTGRES", "raw", "err", Instant.parse("2026-05-18T00:00:00Z"));
    DlqRecord back = mapper.readValue(mapper.writeValueAsString(r), DlqRecord.class);
    assertEquals(r, back);
  }
}
