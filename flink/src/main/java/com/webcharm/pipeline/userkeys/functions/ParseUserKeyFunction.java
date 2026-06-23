package com.webcharm.pipeline.userkeys.functions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.contract.userkeys.event.UserKeyFields;
import com.webcharm.pipeline.userkeys.types.DlqRecord;
import com.webcharm.pipeline.userkeys.types.DlqStage;
import com.webcharm.pipeline.userkeys.types.UserKeyEvent;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Parses a raw Kafka JSON string into a UserKeyEvent. Anything that cannot be stored or aggregated —
 * malformed JSON, a missing id/eventTime/userId/key, a non-numeric value, or a NUL byte in userId or
 * key (which Postgres text columns reject) — is emitted to PARSE_ERROR_TAG as a DlqRecord. Diverting
 * these here, upstream of the exactly-once sink, keeps the sink's input strictly writable so a poison
 * row can never stall the windowed-aggregate commit.
 */
public class ParseUserKeyFunction extends ProcessFunction<String, UserKeyEvent> {

  /** Code point of the NUL character, which Postgres text/varchar columns cannot store. */
  private static final int NUL_CODE_POINT = 0;

  public static final OutputTag<DlqRecord> PARSE_ERROR_TAG =
      new OutputTag<DlqRecord>("parse-error") {};

  private transient ObjectMapper mapper;

  @Override
  public void open(OpenContext openContext) {
    mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public void processElement(String value, Context ctx, Collector<UserKeyEvent> out) {
    try {
      out.collect(parse(value));
    } catch (Exception e) {
      ctx.output(PARSE_ERROR_TAG, new DlqRecord(DlqStage.PARSE, value, e.getMessage(), Instant.now()));
    }
  }

  /**
   * Parses a raw JSON string into a UserKeyEvent. id and eventTime are taken verbatim from the
   * message; userId and key must be present, non-blank, and free of NUL bytes; value must be numeric.
   * Any violation throws, routing the record to the DLQ rather than crashing the task.
   */
  UserKeyEvent parse(String raw) throws Exception {
    Map<String, Object> event = mapper.readValue(raw, new TypeReference<Map<String, Object>>() {});

    UUID id = UUID.fromString(requiredText(event, UserKeyFields.ID));
    Instant eventTime = Instant.parse(requiredText(event, UserKeyFields.EVENT_TIME));
    String userId = storableText(event, UserKeyFields.USER_ID);
    String key = storableText(event, UserKeyFields.KEY);
    long value = numericValue(event, UserKeyFields.VALUE);

    return new UserKeyEvent(id, userId, key, value, eventTime);
  }

  /** Returns a required string field, throwing if absent, null, or blank. */
  private static String requiredText(Map<String, Object> event, String field) {
    Object raw = event.get(field);
    if (raw == null || raw.toString().isBlank()) {
      throw new IllegalArgumentException("missing required field: " + field);
    }
    return raw.toString();
  }

  /** Returns a required string field that is also storable in a Postgres text column (no NUL byte). */
  private static String storableText(Map<String, Object> event, String field) {
    String text = requiredText(event, field);
    if (text.indexOf(NUL_CODE_POINT) >= 0) {
      throw new IllegalArgumentException("field contains a NUL byte, not storable as text: " + field);
    }
    return text;
  }

  /** Returns a required numeric field as a long, throwing if absent or not a number. */
  private static long numericValue(Map<String, Object> event, String field) {
    Object raw = event.get(field);
    if (raw == null) {
      throw new IllegalArgumentException("missing required field: " + field);
    }
    if (raw instanceof Number number) {
      return number.longValue();
    }
    throw new IllegalArgumentException("field is not numeric: " + field + "=" + raw);
  }
}
