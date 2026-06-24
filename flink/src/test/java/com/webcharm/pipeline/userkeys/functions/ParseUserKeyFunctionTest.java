package com.webcharm.pipeline.userkeys.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.webcharm.pipeline.common.dlq.DlqRecord;
import com.webcharm.pipeline.userkeys.types.DlqStage;
import com.webcharm.pipeline.userkeys.types.UserKeyEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

/** Unit tests for ParseUserKeyFunction — exercises JSON parsing and DLQ routing without a Flink runtime. */
class ParseUserKeyFunctionTest {

  /**
   * Harness that extends ParseUserKeyFunction to reach the ProcessFunction.Context inner class,
   * capturing main and side outputs without a Flink mini-cluster.
   */
  static class TestableFn extends ParseUserKeyFunction {
    final List<DlqRecord> dlqCapture = new ArrayList<>();
    final List<UserKeyEvent> mainCapture = new ArrayList<>();

    TestableFn() {
      open(DefaultOpenContext.INSTANCE);
    }

    void run(String json) {
      Context ctx = new Context() {
        @Override public Long timestamp() { return null; }
        @Override public TimerService timerService() { return null; }
        @Override public <X> void output(OutputTag<X> tag, X val) {
          if (val instanceof DlqRecord r) dlqCapture.add(r);
        }
      };
      Collector<UserKeyEvent> col = new Collector<>() {
        public void collect(UserKeyEvent e) { mainCapture.add(e); }
        public void close() {}
      };
      processElement(json, ctx, col);
    }
  }

  /** A well-formed event is parsed into a UserKeyEvent with every field mapped. */
  @Test
  void processElement_validEvent_parsesAllFields() {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000001",
          "eventTime": "2024-01-15T10:00:00Z",
          "source": "ui",
          "userId": "user-1",
          "key": "A",
          "value": 250
        }
        """);

    assertEquals(1, fn.mainCapture.size());
    assertTrue(fn.dlqCapture.isEmpty());
    UserKeyEvent result = fn.mainCapture.get(0);
    assertEquals("user-1", result.getUserId());
    assertEquals("A", result.getKey());
    assertEquals(250L, result.getValue());
    assertEquals(Instant.parse("2024-01-15T10:00:00Z"), result.getEventTime());
  }

  /** Unrecognised JSON fields are ignored; the event still parses and nothing is dead-lettered. */
  @Test
  void processElement_unknownFields_ignored() {
    TestableFn fn = new TestableFn();
    fn.run("""
        {
          "id": "00000000-0000-0000-0000-000000000002",
          "eventTime": "2024-01-15T10:00:00Z",
          "userId": "user-1",
          "key": "A",
          "value": 1,
          "futureField": "x"
        }
        """);

    assertEquals(1, fn.mainCapture.size());
    assertTrue(fn.dlqCapture.isEmpty());
  }

  /** A non-UUID id is dead-lettered at the PARSE stage; the main output stays empty. */
  @Test
  void processElement_malformedUuid_routesToDlq() {
    TestableFn fn = new TestableFn();
    fn.run("""
        {"id": "not-a-uuid", "eventTime": "2024-01-15T10:00:00Z", "userId": "u", "key": "A", "value": 1}
        """);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
    assertEquals(DlqStage.PARSE.name(), fn.dlqCapture.get(0).stage());
    assertNotNull(fn.dlqCapture.get(0).failedAt());
  }

  /** Each missing required field is dead-lettered rather than backfilled. */
  @Test
  void processElement_missingRequiredFields_routeToDlq() {
    String[] missingEach = {
        "{\"eventTime\": \"2024-01-15T10:00:00Z\", \"userId\": \"u\", \"key\": \"A\", \"value\": 1}",
        "{\"id\": \"00000000-0000-0000-0000-000000000003\", \"userId\": \"u\", \"key\": \"A\", \"value\": 1}",
        "{\"id\": \"00000000-0000-0000-0000-000000000003\", \"eventTime\": \"2024-01-15T10:00:00Z\", \"key\": \"A\", \"value\": 1}",
        "{\"id\": \"00000000-0000-0000-0000-000000000003\", \"eventTime\": \"2024-01-15T10:00:00Z\", \"userId\": \"u\", \"value\": 1}",
        "{\"id\": \"00000000-0000-0000-0000-000000000003\", \"eventTime\": \"2024-01-15T10:00:00Z\", \"userId\": \"u\", \"key\": \"A\"}",
    };
    for (String json : missingEach) {
      TestableFn fn = new TestableFn();
      fn.run(json);
      assertEquals(1, fn.dlqCapture.size(), "expected DLQ for: " + json);
      assertTrue(fn.mainCapture.isEmpty(), "expected no main output for: " + json);
    }
  }

  /** Blank userId or key is dead-lettered. */
  @Test
  void processElement_blankUserIdOrKey_routesToDlq() {
    TestableFn fn = new TestableFn();
    fn.run("""
        {"id": "00000000-0000-0000-0000-000000000004", "eventTime": "2024-01-15T10:00:00Z",
         "userId": "  ", "key": "A", "value": 1}
        """);
    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }

  /** A non-numeric value is dead-lettered; the sum aggregate only accepts numbers. */
  @Test
  void processElement_nonNumericValue_routesToDlq() {
    TestableFn fn = new TestableFn();
    fn.run("""
        {"id": "00000000-0000-0000-0000-000000000005", "eventTime": "2024-01-15T10:00:00Z",
         "userId": "u", "key": "A", "value": "not-a-number"}
        """);
    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
    assertEquals(DlqStage.PARSE.name(), fn.dlqCapture.get(0).stage());
  }

  /**
   * A userId carrying a NUL byte is dead-lettered before the sink. Postgres text columns reject NUL,
   * so diverting it here keeps the exactly-once XA sink's input strictly writable. The NUL arrives as
   * a JSON \\u0000 escape, which the parser decodes into a real NUL character that the guard catches.
   */
  @Test
  void processElement_nulEscapeInUserId_routesToDlq() {
    String nulEscape = "\\" + "u0000";
    TestableFn fn = new TestableFn();
    fn.run("{\"id\": \"00000000-0000-0000-0000-000000000006\","
        + "\"eventTime\": \"2024-01-15T10:00:00Z\","
        + "\"userId\": \"u" + nulEscape + "x\", \"key\": \"A\", \"value\": 1}");

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
    assertTrue(fn.dlqCapture.get(0).error().contains("NUL"));
  }

  /** Completely invalid JSON is dead-lettered rather than crashing the task. */
  @Test
  void processElement_invalidJson_routesToDlq() {
    TestableFn fn = new TestableFn();
    fn.run("not-valid-json");
    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }

  /** Parsing the same event twice yields identical fields, so a checkpoint replay reproduces it. */
  @Test
  void processElement_sameInputParsedTwice_isDeterministic() {
    String json = """
        {"id": "00000000-0000-0000-0000-000000000007", "eventTime": "2024-01-15T10:00:00Z",
         "userId": "u", "key": "A", "value": 42}
        """;
    TestableFn first = new TestableFn();
    first.run(json);
    TestableFn second = new TestableFn();
    second.run(json);

    UserKeyEvent a = first.mainCapture.get(0);
    UserKeyEvent b = second.mainCapture.get(0);
    assertEquals(a.getId(), b.getId());
    assertEquals(a.getValue(), b.getValue());
    assertEquals(a.getEventTime(), b.getEventTime());
  }
}
