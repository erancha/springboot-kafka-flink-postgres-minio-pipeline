package com.webcharm.pipeline.common.dlq;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/** Shared factory for the dead-letter Kafka sink used by every pipeline. */
public final class DlqSink {
  private DlqSink() {}

  /**
   * Builds a KafkaSink that writes DlqRecords to the given topic with AT_LEAST_ONCE delivery.
   * AT_LEAST_ONCE flushes pending records at each Flink checkpoint, so a task restart after
   * emitting a DLQ record but before the checkpoint cannot silently drop it. Duplicates are
   * acceptable on the DLQ; silent loss is not.
   */
  public static KafkaSink<DlqRecord> build(String kafkaBootstrap, String dlqTopic) {
    return KafkaSink.<DlqRecord>builder()
        .setBootstrapServers(kafkaBootstrap)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<DlqRecord>builder()
                .setTopic(dlqTopic)
                .setValueSerializationSchema(new DlqRecordSerializer())
                .build())
        .build();
  }
}
