package com.webcharm.pipeline.common.dlq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Serializes a DlqRecord to JSON bytes for the dead-letter Kafka topic. */
public class DlqRecordSerializer implements SerializationSchema<DlqRecord> {
  private transient ObjectMapper mapper;

  @Override
  public void open(InitializationContext context) {
    mapper = new ObjectMapper().registerModule(new JavaTimeModule());
  }

  @Override
  public byte[] serialize(DlqRecord record) {
    try {
      return mapper.writeValueAsBytes(record);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
