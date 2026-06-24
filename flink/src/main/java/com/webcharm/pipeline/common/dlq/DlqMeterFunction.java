package com.webcharm.pipeline.common.dlq;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/**
 * Pass-through operator that counts dead-letter records per originating stage so total DLQ volume
 * and per-stage failure rate are exported as Flink metrics scrapeable by Prometheus. Emits every
 * record unchanged so the downstream DLQ Kafka sink is behaviorally unaffected.
 *
 * Each pipeline supplies its own DlqStage enum class. One counter per stage is pre-registered at
 * open, keyed by the stage's enum-constant name, so a stage that never fails still reports 0 instead
 * of a missing Prometheus series.
 */
public class DlqMeterFunction<E extends Enum<E>> extends RichMapFunction<DlqRecord, DlqRecord> {

  private final Class<E> stageType;
  // Key: DlqStage enum-constant name carried on each DlqRecord. Value: that stage's record counter.
  private transient Map<String, Counter> counters;

  public DlqMeterFunction(Class<E> stageType) {
    this.stageType = stageType;
  }

  @Override
  public void open(OpenContext openContext) {
    counters = createCounters();
  }

  /**
   * Registers one Counter per stage, each tagged with a Prometheus stage label, under the operator
   * metric group. Overridable so tests can supply inspectable counters without a cluster.
   */
  protected Map<String, Counter> createCounters() {
    MetricGroup base = getRuntimeContext().getMetricGroup();
    Map<String, Counter> result = new HashMap<>();
    for (E stage : stageType.getEnumConstants()) {
      result.put(stage.name(), base.addGroup("stage", stage.name()).counter("dlq_records"));
    }
    return result;
  }

  @Override
  public DlqRecord map(DlqRecord record) {
    counters.get(record.stage()).inc();
    return record;
  }
}
