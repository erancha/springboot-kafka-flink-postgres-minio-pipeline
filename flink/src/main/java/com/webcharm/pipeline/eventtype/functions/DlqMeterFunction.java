package com.webcharm.pipeline.eventtype.functions;

import com.webcharm.pipeline.eventtype.types.DlqRecord;
import com.webcharm.pipeline.eventtype.types.DlqStage;
import java.util.EnumMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/**
 * Pass-through operator that counts dead-letter records per originating DlqStage so total DLQ
 * volume and per-stage failure rate (notably IMAGE_ENRICH enrichment failures) are exported as
 * Flink metrics and scrapeable by Prometheus. Emits every record unchanged so the downstream
 * DLQ Kafka sink is behaviorally unaffected.
 */
public class DlqMeterFunction extends RichMapFunction<DlqRecord, DlqRecord> {

  private transient EnumMap<DlqStage, Counter> counters;

  @Override
  public void open(OpenContext openContext) {
    counters = createCounters();
  }

  /**
   * Registers one Counter per DlqStage, each tagged with a Prometheus stage label, under the
   * operator metric group. Overridable so tests can supply inspectable counters without a cluster.
   */
  protected EnumMap<DlqStage, Counter> createCounters() {
    MetricGroup base = getRuntimeContext().getMetricGroup();
    EnumMap<DlqStage, Counter> result = new EnumMap<>(DlqStage.class);
    for (DlqStage stage : DlqStage.values()) {
      result.put(stage, base.addGroup("stage", stage.name()).counter("dlq_records"));
    }
    return result;
  }

  @Override
  public DlqRecord map(DlqRecord record) {
    counters.get(record.stage()).inc();
    return record;
  }
}
