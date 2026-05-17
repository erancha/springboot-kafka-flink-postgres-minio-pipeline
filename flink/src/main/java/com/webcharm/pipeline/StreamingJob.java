package com.webcharm.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.webcharm.pipeline.config.EnvConfig;
import com.webcharm.pipeline.functions.ParseEventFunction;
import com.webcharm.pipeline.functions.MinioUploadFunction;
import com.webcharm.pipeline.functions.PostgresCount5mWriteFunction;
import com.webcharm.pipeline.functions.PostgresWriteFunction;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.EventTypeCount5m;
import com.webcharm.pipeline.types.ProcessedEvent;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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

    StreamExecutionEnvironment executionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    executionEnv.enableCheckpointing(10_000);

    // Retry indefinitely with exponential back-off (5 s → 10 min); let Flink HA
    // manage job-level failover rather than giving up after N attempts.
    // RestartStrategies / Time were removed in Flink 2.x; configure via Configuration instead.
    Configuration restartCfg = new Configuration();
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofSeconds(5));
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofMinutes(10));
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 2.0);
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD,
        Duration.ofMinutes(10));
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR, 0.1);
    executionEnv.configure(restartCfg);

    CheckpointConfig ckptConfig = executionEnv.getCheckpointConfig();
    ckptConfig.setMinPauseBetweenCheckpoints(5_000); // at least 5 s idle between checkpoints to reduce back-pressure
    ckptConfig.setCheckpointTimeout(60_000); // abort a checkpoint that does not complete within 60 s
    ckptConfig.setMaxConcurrentCheckpoints(1); // disallow overlapping checkpoints; one in-flight at a time
    ckptConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION); // keep last checkpoint on cancellation for recovery

    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(kafkaBootstrap)
        .setTopics(kafkaTopic)
        .setGroupId("flink-processor")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        // Disable Kafka auto-commit; offsets are committed only when a Flink checkpoint succeeds,
        // so the committed offset reflects exactly what has been durably processed.
        .setProperty("enable.auto.commit", "false")
        .build();

    // =========== happy path ===========
    // noWatermarks() intentional: the real strategy is assigned below after parsing, where event timestamps are available.
    DataStream<String> rawEvents = executionEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-events");

    SingleOutputStreamOperator<ProcessedEvent> parsedEvents = rawEvents
        .process(new ParseEventFunction())
        .name("parse-json");
    // ===================================

    parsedEvents.getSideOutput(ParseEventFunction.PARSE_ERROR_TAG) // =========== unhappy path: unparseable messages (bad JSON, missing fields)
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic))
        .name("parse-errors-to-dlq");

    // =========== happy path: stamp parsed events with event-time watermarks ===========
    DataStream<ProcessedEvent> timedEvents = parsedEvents
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<ProcessedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli())
                // Without idleness, a partition that goes silent after a burst holds the global
                // watermark frozen — windows never fire. Marking idle partitions excluded lets
                // the watermark advance on active partitions (or to Long.MAX_VALUE when all idle).
                .withIdleness(Duration.ofMinutes(1)))
        .name("event-time-watermarks");
    // =====================================================================================

    SingleOutputStreamOperator<ProcessedEvent> imageEvents = timedEvents
        .filter(e -> "IMAGE".equals(e.getEventType()))
        .process(new MinioUploadFunction()) // =========== happy path: IMAGE events with imageObjectKey set
        .name("minio-upload");

    imageEvents.getSideOutput(MinioUploadFunction.UPLOAD_ERROR_TAG) // =========== unhappy path: MinIO fetch/upload failures (unreachable URL, size cap, etc.)
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic))
        .name("minio-errors-to-dlq");

    imageEvents
        .process(new PostgresWriteFunction()) // =========== happy path: Postgres write is a side effect; replay cannot fix a bad payload
        .name("image-to-postgres")
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic)) // =========== unhappy path: fires only on PermanentJdbcException
        .name("postgres-image-errors-to-dlq");

    timedEvents
        .filter(e -> "DATA".equals(e.getEventType()))
        .process(new PostgresWriteFunction()) // =========== happy path: Postgres write is a side effect
        .name("data-to-postgres")
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic)) // =========== unhappy path: fires only on PermanentJdbcException
        .name("postgres-data-errors-to-dlq");

    // Payload fields are stripped before keying and windowing to keep checkpoint state small.
    buildWindowedCounts(timedEvents)
        .process(new PostgresCount5mWriteFunction()) // =========== happy path: Postgres write is a side effect
        .name("counts-5m-to-postgres")
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic)) // =========== unhappy path: fires only on PermanentJdbcException
        .name("postgres-count-errors-to-dlq");

    log.info("Starting StreamingJob: bootstrap={} topic={}", kafkaBootstrap, kafkaTopic);
    executionEnv.execute("Kafka->Flink->(MinIO,Postgres)");
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
            null, null, null, e.getDate()))
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
