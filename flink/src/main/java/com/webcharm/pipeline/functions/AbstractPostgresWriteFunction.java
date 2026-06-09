package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.webcharm.pipeline.sinks.JdbcWriter;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.DlqStage;
import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Template Method base for ProcessFunctions that write to Postgres and route permanent JDBC failures
 * to a DLQ. Owns open/processElement/close, checkpoint-aligned flushing, and DLQ emission; subclasses
 * supply the writer and a log-context string.
 *
 * The writer buffers rows and flushes them as a committed batch. snapshotState() flushes before the
 * checkpoint completes, so Kafka offsets never advance past unflushed rows (at-least-once; idempotent
 * upserts make replay safe). A permanently-failed row surfaced by a flush is emitted as a DlqRecord;
 * when the flush runs inside snapshotState (no Collector), the record is stashed, persisted to list
 * state, and emitted on the next element — so a restart in that window does not drop the audit row.
 * Transient failures propagate to trigger checkpoint replay.
 */
abstract class AbstractPostgresWriteFunction<T> extends ProcessFunction<T, DlqRecord>
    implements CheckpointedFunction {

  private static final Logger log = LoggerFactory.getLogger(AbstractPostgresWriteFunction.class);
  private static final String DLQ_STATE_NAME = "pending-dlq";

  private transient JdbcWriter<T> writer;
  private transient ObjectMapper mapper;
  private transient PendingDlqBuffer pendingDlq;
  /** Persists pendingDlq as JSON across restarts; String elements avoid any DlqRecord state serializer. */
  private transient ListState<String> pendingDlqState;
  private final DlqStage stage;

  protected AbstractPostgresWriteFunction(DlqStage stage) {
    this.stage = stage;
  }

  protected abstract JdbcWriter<T> createWriter();

  /** Returns a log-friendly string identifying record (e.g. "event id=abc" or "count type=DATA start=..."). */
  protected abstract String logContextString(T record);

  @Override
  public void open(OpenContext openContext) {
    ensureLocalState();
    writer = createWriter();
  }

  /** Creates the non-state-backed locals, idempotently — initializeState runs before open, and unit
      tests drive open directly. */
  private void ensureLocalState() {
    if (mapper == null) {
      mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }
    if (pendingDlq == null) {
      pendingDlq = new PendingDlqBuffer();
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    ensureLocalState();
    pendingDlqState = context.getOperatorStateStore()
        .getListState(new ListStateDescriptor<>(DLQ_STATE_NAME, String.class));
    if (context.isRestored()) {
      pendingDlq.restoreFrom(pendingDlqState.get(), mapper);
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    flushAndStash();
    if (pendingDlqState != null) {
      pendingDlqState.update(pendingDlq.toJson(mapper));
    }
  }

  /** Package-private so the flush/stash path is unit-testable without a Flink context. */
  void flushAndStash() throws Exception {
    for (JdbcWriter.FailedRow<T> failure : writer.flush()) {
      pendingDlq.add(toDlqRecord(failure));
    }
  }

  @Override
  public void processElement(T record, Context ctx, Collector<DlqRecord> out) throws Exception {
    for (DlqRecord deferred : pendingDlq.drain()) {
      out.collect(deferred);
    }
    for (JdbcWriter.FailedRow<T> failure : writer.write(record)) {
      log.error("Permanent JDBC failure for {}, routing to DLQ: {}",
          logContextString(failure.value()), failure.reason());
      out.collect(toDlqRecord(failure));
    }
  }

  private DlqRecord toDlqRecord(JdbcWriter.FailedRow<T> failure) throws JsonProcessingException {
    return new DlqRecord(stage, mapper.writeValueAsString(failure.value()), failure.reason(), Instant.now());
  }

  /**
   * Flushes buffered rows before releasing the writer. A permanent failure here cannot reach the DLQ
   * (no Collector at shutdown), so it is logged rather than emitted.
   */
  @Override
  public void close() throws Exception {
    if (writer == null) {
      return;
    }
    try {
      for (JdbcWriter.FailedRow<T> failure : writer.flush()) {
        log.error("Permanent JDBC failure for {} during close; DLQ unavailable at shutdown: {}",
            logContextString(failure.value()), failure.reason());
      }
    } finally {
      writer.close();
    }
  }
}
