package com.webcharm.pipeline.functions;

import static org.junit.jupiter.api.Assertions.*;

import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.DlqStage;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

/**
 * Verifies AbstractPostgresWriteFunction stamps the constructor-injected DlqStage onto the
 * DlqRecord it emits on a permanent JDBC failure.
 */
class AbstractPostgresWriteFunctionStageTest {

  /** A writer whose every write returns a permanent failure, to force the DLQ branch. */
  private static final class AlwaysPermanentWriter implements JdbcWriter<String> {
    @Override public List<FailedRow<String>> write(String value) {
      return List.of(new FailedRow<>(value, "constraint violation"));
    }
    @Override public List<FailedRow<String>> flush() { return List.of(); }
    @Override public void close() {}
  }

  /**
   * Concrete subclass over String that captures emitted DlqRecords; builds the Context from
   * inside the subclass so the ProcessFunction enclosing instance is implicit.
   */
  private static final class Fn extends AbstractPostgresWriteFunction<String> {
    final List<DlqRecord> out = new ArrayList<>();

    Fn(DlqStage stage) {
      super(stage);
    }

    @Override protected JdbcWriter<String> createWriter() { return new AlwaysPermanentWriter(); }
    @Override protected String logContextString(String record) { return "test"; }

    List<DlqRecord> run(String record) throws Exception {
      open(DefaultOpenContext.INSTANCE);
      Context ctx = new Context() {
        @Override public Long timestamp() { return null; }
        @Override public TimerService timerService() { return null; }
        @Override public <X> void output(OutputTag<X> tag, X val) {}
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
  void stampsImagePostgresStage() throws Exception {
    List<DlqRecord> out = new Fn(DlqStage.IMAGE_POSTGRES).run("payload");
    assertEquals(1, out.size());
    assertEquals(DlqStage.IMAGE_POSTGRES, out.get(0).stage());
  }

  @Test
  void stampsDataPostgresStage() throws Exception {
    List<DlqRecord> out = new Fn(DlqStage.DATA_POSTGRES).run("payload");
    assertEquals(1, out.size());
    assertEquals(DlqStage.DATA_POSTGRES, out.get(0).stage());
  }
}
