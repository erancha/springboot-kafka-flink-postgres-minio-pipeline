package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.ProcessedEvent;
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

  /** Sentinel eventType for an event that is not DATA or IMAGE (invalid value or missing field). */
  private static final String UNEXPECTED = "UNEXPECTED";

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
      if (UNEXPECTED.equals(event.getEventType())) {
        log.error("Unexpected eventType (not DATA/IMAGE): the backend validates eventType as an "
            + "enum and Kafka is private, so this indicates a non-backend producer or schema "
            + "skew. Routing to DLQ.");
        ctx.output(PARSE_ERROR_TAG,
            new DlqRecord(DlqStage.PARSE, value, "unexpected eventType (not DATA/IMAGE)", Instant.now()));
        return;
      }
      if ("IMAGE".equals(event.getEventType())
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
   * imageObjectKey. An eventType that is not DATA or IMAGE - including a missing field - is
   * folded to the UNEXPECTED sentinel.
   */
  @SuppressWarnings("unchecked")
  ProcessedEvent parse(String value) throws Exception {
    Map<String, Object> event = mapper.readValue(value, new TypeReference<Map<String, Object>>() {});

    String id = String.valueOf(event.getOrDefault("id", UUID.randomUUID().toString()));
    String rawType = String.valueOf(event.getOrDefault("eventType", "")).toUpperCase();
    String eventType = ("DATA".equals(rawType) || "IMAGE".equals(rawType)) ? rawType : UNEXPECTED;
    String eventTimeStr = String.valueOf(event.getOrDefault("eventTime", Instant.now().toString()));
    Instant eventTime = Instant.parse(eventTimeStr);
    String sourceName = String.valueOf(event.getOrDefault("source", "unknown"));

    Map<String, Object> payload = null;
    if (event.get("payload") instanceof Map<?, ?> m) {
      payload = (Map<String, Object>) m;
    }

    String imageUrl = Optional.ofNullable(event.get("imageUrl")).map(Object::toString).orElse(null);
    String imageObjectKey = Optional.ofNullable(event.get("imageObjectKey"))
        .map(Object::toString).orElse(null);

    LocalDate date = eventTime.atZone(ZoneOffset.UTC).toLocalDate();

    return new ProcessedEvent(
        UUID.fromString(id), eventType, eventTime, sourceName,
        payload, imageUrl, imageObjectKey, date);
  }
}
