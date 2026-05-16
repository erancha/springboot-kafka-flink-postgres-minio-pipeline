package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.types.DlqRecord;
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

/**
 * Parses a raw Kafka JSON string into a ProcessedEvent. Malformed messages are emitted to
 * PARSE_ERROR_TAG (side output) as DlqRecords rather than crashing the task.
 */
public class ParseEventFunction extends ProcessFunction<String, ProcessedEvent> {

  public static final OutputTag<DlqRecord> PARSE_ERROR_TAG =
      new OutputTag<DlqRecord>("parse-error") {};

  private transient ObjectMapper mapper;

  @Override
  public void open(OpenContext openContext) {
    mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  /**
   * Parses the raw JSON string; routes IMAGE events with neither imageUrl nor imageObjectKey to
   * PARSE_ERROR_TAG; on any parse failure emits a DlqRecord to PARSE_ERROR_TAG instead of
   * propagating the exception.
   */
  @Override
  public void processElement(String value, Context ctx, Collector<ProcessedEvent> out) {
    try {
      ProcessedEvent event = parse(value);
      if ("IMAGE".equals(event.getEventType())
          && event.getImageUrl() == null
          && event.getImageObjectKey() == null) {
        ctx.output(PARSE_ERROR_TAG,
            new DlqRecord(value, "IMAGE event has neither imageUrl nor imageObjectKey", Instant.now()));
        return;
      }
      out.collect(event);
    } catch (Exception e) {
      ctx.output(PARSE_ERROR_TAG, new DlqRecord(value, e.getMessage(), Instant.now()));
    }
  }

  /** Parses a raw JSON string into a ProcessedEvent, extracting all known fields including imageObjectKey. */
  @SuppressWarnings("unchecked")
  ProcessedEvent parse(String value) throws Exception {
    Map<String, Object> event = mapper.readValue(value, new TypeReference<Map<String, Object>>() {});

    String id = String.valueOf(event.getOrDefault("id", UUID.randomUUID().toString()));
    String eventType = String.valueOf(event.getOrDefault("eventType", "UNKNOWN")).toUpperCase();
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
