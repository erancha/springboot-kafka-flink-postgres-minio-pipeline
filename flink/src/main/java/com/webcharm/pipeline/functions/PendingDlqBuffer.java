package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.types.DlqRecord;
import java.util.ArrayList;
import java.util.List;

/**
 * In-memory buffer for dead-letter records discovered during a checkpoint flush, when no Collector is
 * available to emit them. They are drained and emitted on the next processElement; toJson/restoreFrom
 * persist them through Flink list state so a restart before that emission does not drop the audit row.
 */
final class PendingDlqBuffer {

  private final List<DlqRecord> pending = new ArrayList<>();

  void add(DlqRecord record) {
    pending.add(record);
  }

  boolean isEmpty() {
    return pending.isEmpty();
  }

  /** Returns the buffered records and clears the buffer. */
  List<DlqRecord> drain() {
    List<DlqRecord> drained = List.copyOf(pending);
    pending.clear();
    return drained;
  }

  /** Serializes the currently-buffered records to JSON for list-state persistence. */
  List<String> toJson(ObjectMapper mapper) throws JsonProcessingException {
    List<String> out = new ArrayList<>(pending.size());
    for (DlqRecord record : pending) {
      out.add(mapper.writeValueAsString(record));
    }
    return out;
  }

  /** Replaces the buffer contents from persisted JSON on restore. */
  void restoreFrom(Iterable<String> json, ObjectMapper mapper) throws JsonProcessingException {
    pending.clear();
    for (String entry : json) {
      pending.add(mapper.readValue(entry, DlqRecord.class));
    }
  }
}
