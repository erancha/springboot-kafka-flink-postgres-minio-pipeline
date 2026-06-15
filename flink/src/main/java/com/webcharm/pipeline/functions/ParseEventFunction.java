package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.EventType;
import com.webcharm.pipeline.types.ProcessedEvent;
import com.webcharm.contract.eventtype.event.EventFields;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses a raw Kafka JSON string into a ProcessedEvent. Malformed messages, and events whose
 * eventType is not DATA or IMAGE, are emitted to PARSE_ERROR_TAG (side output) as DlqRecords
 * rather than crashing the task.
 */
public class ParseEventFunction extends ProcessFunction<String, ProcessedEvent> {

  private static final Logger log = LoggerFactory.getLogger(ParseEventFunction.class);

  public static final OutputTag<DlqRecord> PARSE_ERROR_TAG =
      new OutputTag<DlqRecord>("parse-error") {};

  private transient ObjectMapper mapper;

  @Override
  public void open(OpenContext openContext) {
    mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  /**
   * Parses the raw JSON string and emits the event downstream. Routes to PARSE_ERROR_TAG: an
   * eventType that is not DATA or IMAGE (also logged at error, since a valid backend cannot
   * produce one), an IMAGE event with neither imageUrl nor imageObjectKey, and any parse failure.
   */
  @Override
  public void processElement(String value, Context ctx, Collector<ProcessedEvent> out) {
    try {
      ProcessedEvent event = parse(value);
      if (EventType.UNEXPECTED.equals(event.getEventType())) {
        log.error("Unexpected eventType (not DATA/IMAGE): the backend validates eventType as an "
            + "enum and Kafka is private, so this indicates a non-backend producer or schema "
            + "skew. Routing to DLQ.");
        ctx.output(PARSE_ERROR_TAG,
            new DlqRecord(DlqStage.PARSE, value, "unexpected eventType (not DATA/IMAGE)", Instant.now()));
        return;
      }
      if (EventType.IMAGE.equals(event.getEventType())
          && event.getImageUrl() == null
          && event.getImageObjectKey() == null) {
        ctx.output(PARSE_ERROR_TAG,
            new DlqRecord(DlqStage.PARSE, value, "IMAGE event has neither imageUrl nor imageObjectKey", Instant.now()));
        return;
      }
      out.collect(event);
    } catch (Exception e) {
      ctx.output(PARSE_ERROR_TAG, new DlqRecord(DlqStage.PARSE, value, e.getMessage(), Instant.now()));
    }
  }

  /**
   * Parses a raw JSON string into a ProcessedEvent, extracting all known fields including
   * imageObjectKey. The parse is a pure function of its input: id and eventTime are taken
   * verbatim from the message and a missing or blank value is rejected (thrown, hence routed
   * to the DLQ) rather than backfilled, so a checkpoint replay re-derives the identical
   * upsert key and the id-keyed idempotent write stays effective-exactly-once. An eventType
   * that is not DATA or IMAGE - including a missing field - is folded to the UNEXPECTED
   * sentinel.
   */
  @SuppressWarnings("unchecked")
  ProcessedEvent parse(String value) throws Exception {
    Map<String, Object> event = mapper.readValue(value, new TypeReference<Map<String, Object>>() {});

    String id = required(event, EventFields.ID);
    String rawType = String.valueOf(event.getOrDefault(EventFields.EVENT_TYPE, "")).toUpperCase();
    String eventType = (EventType.DATA.equals(rawType) || EventType.IMAGE.equals(rawType))
        ? rawType : EventType.UNEXPECTED;
    Instant eventTime = Instant.parse(required(event, EventFields.EVENT_TIME));
    String sourceName = String.valueOf(event.getOrDefault(EventFields.SOURCE, "unknown"));

    Map<String, Object> payload = null;
    if (event.get(EventFields.PAYLOAD) instanceof Map<?, ?> m) {
      payload = (Map<String, Object>) m;
    }

    String imageUrl = Optional.ofNullable(event.get(EventFields.IMAGE_URL)).map(Object::toString).orElse(null);
    String imageObjectKey = Optional.ofNullable(event.get(EventFields.IMAGE_OBJECT_KEY))
        .map(Object::toString).orElse(null);

    LocalDate date = eventTime.atZone(ZoneOffset.UTC).toLocalDate();

    return new ProcessedEvent(
        UUID.fromString(id), eventType, eventTime, sourceName,
        payload, imageUrl, imageObjectKey, date);
  }

  /**
   * Returns the value of a required string field, throwing if the field is absent, null, or
   * blank. Used for id and eventTime, whose values must come from the message so the parse
   * is deterministic across a checkpoint replay; a fabricated default would break idempotency.
   */
  private static String required(Map<String, Object> event, String field) {
    Object raw = event.get(field);
    if (raw == null || raw.toString().isBlank()) {
      throw new IllegalArgumentException("missing required field: " + field);
    }
    return raw.toString();
  }
}
