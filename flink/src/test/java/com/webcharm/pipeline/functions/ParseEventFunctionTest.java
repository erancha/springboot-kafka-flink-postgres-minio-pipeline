package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.ProcessedEvent;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for ParseEventFunction — exercises JSON parsing and DLQ routing in isolation without a Flink runtime. */
class ParseEventFunctionTest {

  /**
   * Test harness that extends ParseEventFunction to gain access to the ProcessFunction.Context
   * inner class, enabling side-output capture without a full Flink mini-cluster.
   */
  static class TestableFn extends ParseEventFunction {
    final List<DlqRecord> dlqCapture = new ArrayList<>();
    final List<ProcessedEvent> mainCapture = new ArrayList<>();

    TestableFn() throws Exception {
      open(DefaultOpenContext.INSTANCE);
    }

    void run(String json) throws Exception {
      Context ctx = new Context() {
        @Override public Long timestamp() { return null; }
        @Override public TimerService timerService() { return null; }
        @Override public <X> void output(OutputTag<X> tag, X val) {
          if (val instanceof DlqRecord r) dlqCapture.add(r);
        }
      };
      Collector<ProcessedEvent> col = new Collector<>() {
        public void collect(ProcessedEvent e) { mainCapture.add(e); }
        public void close() {}
      };
      processElement(json, ctx, col);
    }
  }

  /** A well-formed DATA event is parsed into a ProcessedEvent with all fields correctly mapped. */
  @Test
  void map_dataEvent_parsesAllFields() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000001",
          "eventType": "data",
          "eventTime": "2024-01-15T10:00:00Z",
          "source": "ui",
          "payload": {"key": "value"}
        }
        """);

    assertEquals(1, fn.mainCapture.size());
    assertTrue(fn.dlqCapture.isEmpty());
    ProcessedEvent result = fn.mainCapture.get(0);
    assertEquals("DATA", result.getEventType());
    assertEquals("ui", result.getSource());
    assertEquals(Instant.parse("2024-01-15T10:00:00Z"), result.getEventTime());
    assertNotNull(result.getPayload());
    assertEquals("value", result.getPayload().get("key"));
    assertNull(result.getImageUrl());
  }

  /** An IMAGE event containing only imageUrl is parsed correctly; imageObjectKey is null because it was absent from the input. */
  @Test
  void map_imageEventWithUrl_parsesImageUrl() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000002",
          "eventType": "IMAGE",
          "eventTime": "2024-01-15T10:00:00Z",
          "source": "ui",
          "imageUrl": "https://example.com/photo.jpg"
        }
        """);

    assertEquals(1, fn.mainCapture.size());
    assertTrue(fn.dlqCapture.isEmpty());
    ProcessedEvent result = fn.mainCapture.get(0);
    assertEquals("IMAGE", result.getEventType());
    assertEquals("https://example.com/photo.jpg", result.getImageUrl());
    assertNull(result.getImageObjectKey());
  }

  /** Optional fields absent from the JSON default to "unknown" source, "image/jpeg" content type, and null payload. */
  @Test
  void map_missingOptionalFields_usesDefaults() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000003",
          "eventType": "DATA",
          "eventTime": "2024-01-15T10:00:00Z"
        }
        """);

    assertEquals(1, fn.mainCapture.size());
    assertTrue(fn.dlqCapture.isEmpty());
    ProcessedEvent result = fn.mainCapture.get(0);
    assertEquals("unknown", result.getSource());
    assertNull(result.getPayload());
  }

  /** Unrecognised JSON fields do not cause a failure; the event is parsed normally and no DLQ record is emitted. */
  @Test
  void map_unknownFields_ignored() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000004",
          "eventType": "DATA",
          "eventTime": "2024-01-15T10:00:00Z",
          "futureField": "someValue"
        }
        """);

    assertEquals(1, fn.mainCapture.size());
    assertTrue(fn.dlqCapture.isEmpty());
  }

  /** A non-UUID id string causes a DlqRecord to be emitted to the side output; the main output receives nothing. */
  @Test
  void processElement_malformedUuid_emitsToDlqNotMain() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {"id": "not-a-uuid", "eventType": "DATA", "eventTime": "2024-01-15T10:00:00Z"}
        """);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
    assertNotNull(fn.dlqCapture.get(0).error());
    assertNotNull(fn.dlqCapture.get(0).failedAt());
  }

  /** An unparseable eventTime string causes a DlqRecord to be emitted to the side output; the main output receives nothing. */
  @Test
  void processElement_malformedTimestamp_emitsToDlqNotMain() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {"id": "00000000-0000-0000-0000-000000000001", "eventType": "DATA", "eventTime": "not-a-timestamp"}
        """);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }

  /** A completely invalid JSON string causes a DlqRecord to be emitted to the side output; the main output receives nothing. */
  @Test
  void processElement_invalidJson_emitsToDlqNotMain() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("not-valid-json");

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }

  /** An IMAGE event with imageObjectKey is parsed correctly; imageUrl is null because it was absent from the input. */
  @Test
  void map_imageEventWithObjectKey_parsesObjectKey() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000006",
          "eventType": "IMAGE",
          "eventTime": "2024-01-15T10:00:00Z",
          "source": "ui",
          "imageObjectKey": "images/2024-01-15/00000000-0000-0000-0000-000000000006.jpg"
        }
        """);

    assertEquals(1, fn.mainCapture.size());
    assertTrue(fn.dlqCapture.isEmpty());
    ProcessedEvent result = fn.mainCapture.get(0);
    assertEquals("IMAGE", result.getEventType());
    assertEquals("images/2024-01-15/00000000-0000-0000-0000-000000000006.jpg",
        result.getImageObjectKey());
    assertNull(result.getImageUrl());
  }

  /** An IMAGE event with neither imageUrl nor imageObjectKey is routed to the DLQ. */
  @Test
  void processElement_imageEventNeitherUrlNorObjectKey_routesToDlq() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000007",
          "eventType": "IMAGE",
          "eventTime": "2024-01-15T10:00:00Z"
        }
        """);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
    assertNotNull(fn.dlqCapture.get(0).error());
  }

  /** An eventType that is not DATA or IMAGE is routed to the DLQ and never reaches the main output. */
  @Test
  void processElement_invalidEventType_routesToDlq() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000008",
          "eventType": "FOO",
          "eventTime": "2024-01-15T10:00:00Z"
        }
        """);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
    assertTrue(fn.dlqCapture.get(0).error().contains("unexpected eventType"));
    assertEquals(DlqStage.PARSE, fn.dlqCapture.get(0).stage());
  }

  /** A missing eventType field is treated as unexpected and routed to the DLQ. */
  @Test
  void processElement_missingEventType_routesToDlq() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000009",
          "eventTime": "2024-01-15T10:00:00Z"
        }
        """);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }
}
