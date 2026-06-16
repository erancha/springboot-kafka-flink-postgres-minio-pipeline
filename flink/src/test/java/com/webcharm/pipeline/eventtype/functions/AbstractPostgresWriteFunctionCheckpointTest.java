package com.webcharm.pipeline.eventtype.functions;

import static org.junit.jupiter.api.Assertions.*;

import com.webcharm.pipeline.eventtype.sinks.JdbcWriter;
import com.webcharm.pipeline.eventtype.types.DlqRecord;
import com.webcharm.pipeline.eventtype.types.DlqStage;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

/**
 * Verifies the checkpoint-aligned DLQ handling: a permanent failure surfaced by a snapshot flush is
 * stashed and emitted on the next element (no Collector inside snapshotState), while a failure
 * surfaced by an ordinary write is emitted immediately.
 */
class AbstractPostgresWriteFunctionCheckpointTest {

  /** A writer with programmable write/flush results, recording how often flush ran. */
  private static final class FakeWriter implements JdbcWriter<String> {
    List<FailedRow<String>> writeResult = List.of();
    List<FailedRow<String>> flushResult = List.of();
    int flushCalls = 0;

    @Override public List<FailedRow<String>> write(String value) { return writeResult; }
    @Override public List<FailedRow<String>> flush() { flushCalls++; return flushResult; }
    @Override public void close() {}
  }

  /** Concrete subclass over String; builds the Context from inside so the enclosing instance is implicit. */
  private static final class Fn extends AbstractPostgresWriteFunction<String> {
    final FakeWriter writer;

    Fn(FakeWriter writer) {
      super(DlqStage.DATA_POSTGRES);
      this.writer = writer;
    }

    @Override protected JdbcWriter<String> createWriter() { return writer; }
    @Override protected String logContextString(String record) { return record; }

    List<DlqRecord> process(String record) throws Exception {
      List<DlqRecord> out = new ArrayList<>();
      Context ctx = new Context() {
        @Override public Long timestamp() { return null; }
        @Override public TimerService timerService() { return null; }
        @Override public <X> void output(OutputTag<X> tag, X value) {}
      };
      Collector<DlqRecord> col = new Collector<>() {
        public void collect(DlqRecord d) { out.add(d); }
        public void close() {}
      };
      processElement(record, ctx, col);
      return out;
    }
  }

  @Test
  void poisonFromCheckpointFlush_isEmittedOnNextElement_thenDrained() throws Exception {
    FakeWriter writer = new FakeWriter();
    Fn fn = new Fn(writer);
    fn.open(DefaultOpenContext.INSTANCE);
    writer.flushResult = List.of(new JdbcWriter.FailedRow<>("bad", "constraint"));

    fn.flushAndStash(); // stands in for the flush inside snapshotState
    assertEquals(1, writer.flushCalls);

    List<DlqRecord> emitted = fn.process("next");
    assertEquals(1, emitted.size());
    assertEquals(DlqStage.DATA_POSTGRES, emitted.get(0).stage());
    assertTrue(emitted.get(0).raw().contains("bad"));

    assertTrue(fn.process("again").isEmpty(), "stashed record must be drained exactly once");
  }

  @Test
  void permanentFailureFromOrdinaryWrite_isEmittedImmediately() throws Exception {
    FakeWriter writer = new FakeWriter();
    Fn fn = new Fn(writer);
    fn.open(DefaultOpenContext.INSTANCE);
    writer.writeResult = List.of(new JdbcWriter.FailedRow<>("x", "constraint"));

    List<DlqRecord> emitted = fn.process("x");

    assertEquals(1, emitted.size());
    assertEquals(DlqStage.DATA_POSTGRES, emitted.get(0).stage());
  }
}
