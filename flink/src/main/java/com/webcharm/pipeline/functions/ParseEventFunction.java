package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.types.ProcessedEvent;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

/** Parses a raw Kafka JSON string into a ProcessedEvent; extracted from StreamingJob to be unit-testable in isolation. */
public class ParseEventFunction extends RichMapFunction<String, ProcessedEvent> {

  private transient ObjectMapper mapper;

  @Override
  public void open(OpenContext openContext) {
    mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ProcessedEvent map(String value) throws Exception {
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
