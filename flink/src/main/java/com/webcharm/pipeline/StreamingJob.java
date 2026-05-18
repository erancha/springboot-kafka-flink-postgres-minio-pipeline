package com.webcharm.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.webcharm.pipeline.config.EnvConfig;
import com.webcharm.pipeline.functions.EnrichSplitFunction;
import com.webcharm.pipeline.functions.MinioAsyncImageFunction;
import com.webcharm.pipeline.functions.ParseEventFunction;
import com.webcharm.pipeline.functions.PostgresCount5mWriteFunction;
import com.webcharm.pipeline.functions.PostgresWriteFunction;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.EnrichResult;
import com.webcharm.pipeline.types.EventType;
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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * Defines and runs the pipeline: parse Kafka events, route IMAGE through async MinIO
 * enrichment and DATA straight to Postgres, pre-aggregate 5-minute per-type counts, and
 * send every failure to the dead-letter Kafka topic.
 */
public class StreamingJob {
  private static final Logger log = LoggerFactory.getLogger(StreamingJob.class);

  /** Runs startup pre-flight checks, then builds the job graph and submits it to the Flink runtime. */
  public static void main(String[] args) throws Exception {
    // Fail fast on a misconfigured deployment before submitting the job.
    PreflightChecks.run();

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

    // =========== unhappy path: unparseable messages (bad JSON, missing fields) ===========
    DataStream<DlqRecord> parseErrors =
        parsedEvents.getSideOutput(ParseEventFunction.PARSE_ERROR_TAG);

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

    SingleOutputStreamOperator<ProcessedEvent> imagePipeline = buildImagePipeline(
        timedEvents.filter(e -> EventType.IMAGE.equals(e.getEventType())));

    // =========== unhappy path: image fetch/upload failures or exhausted transient ===========
    DataStream<DlqRecord> minioErrors =
        imagePipeline.getSideOutput(EnrichSplitFunction.UPLOAD_ERROR_TAG);

    // Postgres write is a side effect; replay cannot fix a bad payload, so PermanentJdbcException
    // is emitted as a DlqRecord on the main output rather than propagated.
    DataStream<DlqRecord> imagePostgresErrors = imagePipeline
        .process(new PostgresWriteFunction(DlqStage.IMAGE_POSTGRES))
        .name("image-to-postgres");

    DataStream<DlqRecord> dataPostgresErrors = timedEvents
        .filter(e -> EventType.DATA.equals(e.getEventType()))
        .process(new PostgresWriteFunction(DlqStage.DATA_POSTGRES))
        .name("data-to-postgres");

    // Payload fields are stripped before keying and windowing to keep checkpoint state small.
    DataStream<DlqRecord> countErrors = buildWindowedCounts(timedEvents)
        .process(new PostgresCount5mWriteFunction(DlqStage.COUNT_POSTGRES))
        .name("counts-5m-to-postgres");

    // All dead-letter paths converge into one Kafka producer. union is type-safe (every input is
    // DataStream<DlqRecord>) and behaviour-preserving: each record still reaches events-dlq, and
    // its DlqStage carries the origin that the per-source sink names used to encode. A single
    // sink couples DLQ backpressure across the five branches, but dead-letter volume is low by
    // nature (only failures), so this is acceptable and is the standard pattern.
    parseErrors
        .union(minioErrors, imagePostgresErrors, dataPostgresErrors, countErrors)
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic))
        .name("dlq-sink");

    log.info("Starting StreamingJob: bootstrap={} topic={}", kafkaBootstrap, kafkaTopic);
    executionEnv.execute("Kafka->Flink->(MinIO,Postgres)");
  }

  /**
   * Strips binary and payload fields, keys by eventType, applies 5-minute tumbling event-time
   * windows, and emits one EventTypeCount5m per (type, window) pair when each window closes.
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

  /**
   * Wires the IMAGE branch as a Flink async I/O operator (non-blocking fetch plus MinIO upload,
   * framework-managed exponential-backoff retry on transient failures) followed by a non-blocking
   * split. Returns the split operator whose main output is enriched events and whose
   * EnrichSplitFunction.UPLOAD_ERROR_TAG side output carries DLQ records. The operator timeout
   * is decoupled from the checkpoint timeout because the task thread never blocks.
   */
  static SingleOutputStreamOperator<ProcessedEvent> buildImagePipeline(
      DataStream<ProcessedEvent> imageEvents) {

    int capacity = EnvConfig.envInt("MINIO_ASYNC_CAPACITY", 100);
    int timeoutSecs = EnvConfig.envInt("MINIO_ASYNC_TIMEOUT_SECS", 120);
    int maxAttempts = EnvConfig.envInt("MINIO_ASYNC_MAX_ATTEMPTS", 3);

    SerializablePredicate<Collection<EnrichResult>> retryable =
        results -> results.stream().anyMatch(EnrichResult::isRetryable);
    AsyncRetryStrategy<EnrichResult> retryStrategy =
        new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<EnrichResult>(
                maxAttempts, 1_000L, 4_000L, 2.0)
            .ifResult(retryable)
            .build();

    SingleOutputStreamOperator<EnrichResult> enriched = AsyncDataStream.unorderedWaitWithRetry(
            imageEvents,
            new MinioAsyncImageFunction(),
            timeoutSecs, TimeUnit.SECONDS,
            capacity,
            retryStrategy)
        .name("minio-enrich-async");

    return enriched
        .process(new EnrichSplitFunction())
        .name("enrich-split");
  }

  /**
   * A Predicate that is also Serializable so Flink can ship it in the job graph.
   */
  private interface SerializablePredicate<T> extends Predicate<T>, Serializable {
  }

  /**
   * Builds a KafkaSink that writes DlqRecords to the given topic with AT_LEAST_ONCE delivery.
   * AT_LEAST_ONCE flushes pending records at each Flink checkpoint, so a task restart after
   * emitting a DLQ record but before the checkpoint cannot silently drop it. Duplicates are
   * acceptable on the DLQ; silent loss is not.
   */
  private static KafkaSink<DlqRecord> buildDlqSink(String kafkaBootstrap, String dlqTopic) {
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
