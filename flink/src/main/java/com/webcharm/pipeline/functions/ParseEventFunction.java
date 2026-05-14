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

  /** Parses the raw JSON string; on any parse failure emits a DlqRecord to PARSE_ERROR_TAG instead of propagating the exception. */
  @Override
  public void processElement(String value, Context ctx, Collector<ProcessedEvent> out) {
    try {
      out.collect(parse(value));
    } catch (Exception e) {
      ctx.output(PARSE_ERROR_TAG, new DlqRecord(value, e.getMessage(), Instant.now()));
    }
  }

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
    String imageBase64 = Optional.ofNullable(event.get("imageBase64")).map(Object::toString).orElse(null);
    String imageContentType = Optional.ofNullable(event.get("imageContentType"))
        .map(Object::toString).orElse("image/jpeg");

    LocalDate date = eventTime.atZone(ZoneOffset.UTC).toLocalDate();

    return new ProcessedEvent(
        UUID.fromString(id), eventType, eventTime, sourceName,
        payload, imageUrl, imageBase64, imageContentType, null, date);
  }
}
