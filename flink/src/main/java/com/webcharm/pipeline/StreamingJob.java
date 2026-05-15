package com.webcharm.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.webcharm.pipeline.config.EnvConfig;
import com.webcharm.pipeline.functions.ParseEventFunction;
import com.webcharm.pipeline.functions.MinioUploadFunction;
import com.webcharm.pipeline.sinks.PostgresEventTypeCount5mSink;
import com.webcharm.pipeline.sinks.PostgresProcessedEventSink;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.EventTypeCount5m;
import com.webcharm.pipeline.types.ProcessedEvent;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.StreamSupport;

public class StreamingJob {
  private static final Logger log = LoggerFactory.getLogger(StreamingJob.class);

  public static void main(String[] args) throws Exception {
    String kafkaBootstrap = EnvConfig.env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    String kafkaTopic = EnvConfig.env("KAFKA_TOPIC", "events");
    String dlqTopic = EnvConfig.env("KAFKA_DLQ_TOPIC", "events-dlq");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10_000);

    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(kafkaBootstrap)
        .setTopics(kafkaTopic)
        .setGroupId("flink-processor")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    // noWatermarks() here is intentional: the real strategy is assigned on the typed stream below,
    // after JSON parsing, where event timestamps are available.
    DataStream<String> json = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-events");

    SingleOutputStreamOperator<ProcessedEvent> processed = json
        .process(new ParseEventFunction())
        .name("parse-json");

    // Route unparsable messages to a dead-letter Kafka topic instead of crashing the job.
    processed.getSideOutput(ParseEventFunction.PARSE_ERROR_TAG)
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic))
        .name("parse-errors-to-dlq");

    DataStream<ProcessedEvent> processedWithWatermarks = processed
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<ProcessedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli())
                // Without idleness, a partition that goes silent after a burst holds the global
                // watermark frozen — windows never fire. Marking idle partitions excluded lets
                // the watermark advance on active partitions (or to Long.MAX_VALUE when all idle).
                .withIdleness(Duration.ofMinutes(1)))
        .name("event-time-watermarks");

    // IMAGE path: upload to MinIO (clears base64/url, sets imageObjectKey), then write to Postgres.
    // Upload/decode failures go to the DLQ side output instead of crashing the operator.
    SingleOutputStreamOperator<ProcessedEvent> uploadedImages = processedWithWatermarks
        .filter(e -> "IMAGE".equals(e.getEventType()))
        .process(new MinioUploadFunction())
        .name("minio-upload");

    uploadedImages.getSideOutput(MinioUploadFunction.UPLOAD_ERROR_TAG)
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic))
        .name("minio-errors-to-dlq");

    uploadedImages
        .sinkTo(new PostgresProcessedEventSink())
        .name("image-to-postgres");

    // DATA path: write directly to Postgres
    processedWithWatermarks
        .filter(e -> "DATA".equals(e.getEventType()))
        .sinkTo(new PostgresProcessedEventSink())
        .name("data-to-postgres");

    // Strip all binary/payload fields before keying and windowing: imageBase64 can be several MB and
    // would be serialized into every checkpoint snapshot for each in-flight event in the window.
    buildWindowedCounts(processedWithWatermarks)
        .sinkTo(new PostgresEventTypeCount5mSink())
        .name("counts-5m-to-postgres");

    log.info("Starting StreamingJob: bootstrap={} topic={}", kafkaBootstrap, kafkaTopic);
    env.execute("Kafka->Flink->(MinIO,Postgres)");
  }

  /**
   * Strips binary and payload fields, keys by eventType, applies 5-minute tumbling event-time windows, 
   * and emits one EventTypeCount5m per (type, window) pair when each window closes.
   * Extracted as a package-private static method so StreamingJobIT can exercise it directly
   * with a controlled bounded source — no Kafka or wall-clock waiting required.
   */
  static DataStream<EventTypeCount5m> buildWindowedCounts(DataStream<ProcessedEvent> withWatermarks) {
    return withWatermarks
        .map(e -> new ProcessedEvent(
            e.getId(), e.getEventType(), e.getEventTime(), e.getSource(),
            null, null, null, null, null, e.getDate()))
        .name("strip-for-window")
        .keyBy(ProcessedEvent::getEventType)
        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
        .process(new ProcessWindowFunction<ProcessedEvent, EventTypeCount5m, String, TimeWindow>() {
          @Override
          public void process(String key, Context context, Iterable<ProcessedEvent> elements,
              Collector<EventTypeCount5m> out) {
            long count = StreamSupport.stream(elements.spliterator(), false).count();
            out.collect(new EventTypeCount5m(
                Instant.ofEpochMilli(context.window().getStart()),
                Instant.ofEpochMilli(context.window().getEnd()),
                key,
                count));
          }
        })
        .name("count-by-type-5m");
  }

  /** Builds a KafkaSink that writes DlqRecords to the given topic. Each call returns a new instance. */
  private static KafkaSink<DlqRecord> buildDlqSink(String kafkaBootstrap, String dlqTopic) {
    return KafkaSink.<DlqRecord>builder()
        .setBootstrapServers(kafkaBootstrap)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<DlqRecord>builder()
                .setTopic(dlqTopic)
                .setValueSerializationSchema(new DlqRecordSerializer())
                .build())
        .build();
  }

  /** Serializes DlqRecord to JSON bytes for the dead-letter Kafka topic. */
  private static class DlqRecordSerializer implements SerializationSchema<DlqRecord> {
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
}
