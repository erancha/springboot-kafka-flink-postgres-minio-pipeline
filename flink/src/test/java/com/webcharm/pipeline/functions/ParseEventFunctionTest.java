package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.types.ProcessedEvent;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for ParseEventFunction — exercises JSON parsing in isolation without a Flink runtime. */
class ParseEventFunctionTest {

  ParseEventFunction fn;

  @BeforeEach
  void setUp() throws Exception {
    fn = new ParseEventFunction();
    fn.open(DefaultOpenContext.INSTANCE);
  }

  @Test
  void map_dataEvent_parsesAllFields() throws Exception {
    String json = """
        {
          "id": "00000000-0000-0000-0000-000000000001",
          "eventType": "data",
          "eventTime": "2024-01-15T10:00:00Z",
          "source": "ui",
          "payload": {"key": "value"}
        }
        """;

    ProcessedEvent result = fn.map(json);

    assertEquals("DATA", result.getEventType());
    assertEquals("ui", result.getSource());
    assertEquals(Instant.parse("2024-01-15T10:00:00Z"), result.getEventTime());
    assertNotNull(result.getPayload());
    assertEquals("value", result.getPayload().get("key"));
    assertNull(result.getImageBase64());
    assertNull(result.getImageUrl());
  }

  @Test
  void map_imageEventWithUrl_parsesImageUrl() throws Exception {
    String json = """
        {
          "id": "00000000-0000-0000-0000-000000000002",
          "eventType": "IMAGE",
          "eventTime": "2024-01-15T10:00:00Z",
          "source": "ui",
          "imageUrl": "https://example.com/photo.jpg",
          "imageContentType": "image/jpeg"
        }
        """;

    ProcessedEvent result = fn.map(json);

    assertEquals("IMAGE", result.getEventType());
    assertEquals("https://example.com/photo.jpg", result.getImageUrl());
    assertEquals("image/jpeg", result.getImageContentType());
    assertNull(result.getImageBase64());
  }

  @Test
  void map_missingOptionalFields_usesDefaults() throws Exception {
    String json = """
        {
          "id": "00000000-0000-0000-0000-000000000003",
          "eventType": "DATA",
          "eventTime": "2024-01-15T10:00:00Z"
        }
        """;

    ProcessedEvent result = fn.map(json);

    assertEquals("unknown", result.getSource());
    assertEquals("image/jpeg", result.getImageContentType());
    assertNull(result.getPayload());
  }

  @Test
  void map_unknownFields_ignored() throws Exception {
    String json = """
        {
          "id": "00000000-0000-0000-0000-000000000004",
          "eventType": "DATA",
          "eventTime": "2024-01-15T10:00:00Z",
          "futureField": "someValue"
        }
        """;

    assertDoesNotThrow(() -> fn.map(json));
  }
}
