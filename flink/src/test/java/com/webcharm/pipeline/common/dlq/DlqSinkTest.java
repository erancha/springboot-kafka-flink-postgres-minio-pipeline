package com.webcharm.pipeline.common.dlq;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.junit.jupiter.api.Test;

/**
 * Verifies DlqSink.build assembles a usable dead-letter KafkaSink, so both pipelines share one
 * factory instead of re-declaring the builder chain.
 */
class DlqSinkTest {

  @Test
  void buildsSink() {
    KafkaSink<DlqRecord> sink = DlqSink.build("kafka:9092", "events-dlq");
    assertNotNull(sink);
  }
}
