package com.webcharm.pipeline.eventtype.functions;

import com.webcharm.pipeline.common.dlq.DlqRecord;
import com.webcharm.pipeline.eventtype.types.DlqStage;
import com.webcharm.pipeline.eventtype.types.ProcessedEvent;
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

  /** Optional fields absent from the JSON default to "unknown" source and null payload. */
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

  @Test
  void processElement_malformedTimestamp_emitsToDlqNotMain() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {"id": "00000000-0000-0000-0000-000000000001", "eventType": "DATA", "eventTime": "not-a-timestamp"}
        """);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }

  @Test
  void processElement_invalidJson_emitsToDlqNotMain() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("not-valid-json");

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }

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
    assertEquals(DlqStage.PARSE.name(), fn.dlqCapture.get(0).stage());
  }

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

  /**
   * A missing id is routed to the DLQ rather than backfilled with a random UUID. Fabricating
   * an id makes the parse non-deterministic: a checkpoint replay would emit a different id and
   * the id-keyed upsert would write a duplicate row, breaking the effective-exactly-once
   * guarantee. Rejecting the record keeps the parse a pure function of its input.
   */
  @Test
  void processElement_missingId_routesToDlq() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "eventType": "DATA",
          "eventTime": "2024-01-15T10:00:00Z"
        }
        """);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
    assertEquals(DlqStage.PARSE.name(), fn.dlqCapture.get(0).stage());
  }

  /**
   * A missing eventTime is routed to the DLQ rather than backfilled with Instant.now().
   * Fabricating the time makes the parse non-deterministic: a replay would derive a different
   * eventTime (and partition date), so the id-keyed upsert would overwrite the row with
   * different data and an IMAGE object would land under a different MinIO key.
   */
  @Test
  void processElement_missingEventTime_routesToDlq() throws Exception {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000010",
          "eventType": "DATA"
        }
        """);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
    assertEquals(DlqStage.PARSE.name(), fn.dlqCapture.get(0).stage());
  }

  /**
   * Parsing the same well-formed event twice yields byte-identical id and eventTime, so a
   * checkpoint replay reproduces the same upsert key and the same row — the property the
   * effective-exactly-once claim rests on.
   */
  @Test
  void processElement_sameInputParsedTwice_isDeterministic() throws Exception {
    String json = """
        {
          "id": "00000000-0000-0000-0000-000000000011",
          "eventType": "DATA",
          "eventTime": "2024-01-15T10:00:00Z",
          "source": "ui"
        }
        """;
    TestableFn first = new TestableFn();
    first.run(json);
    TestableFn second = new TestableFn();
    second.run(json);

    assertEquals(1, first.mainCapture.size());
    assertEquals(1, second.mainCapture.size());
    ProcessedEvent a = first.mainCapture.get(0);
    ProcessedEvent b = second.mainCapture.get(0);
    assertEquals(a.getId(), b.getId());
    assertEquals(a.getEventTime(), b.getEventTime());
  }
}
