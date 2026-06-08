package com.webcharm.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.webcharm.pipeline.config.EnvConfig;
import com.webcharm.pipeline.functions.CountAggregateFunction;
import com.webcharm.pipeline.functions.DlqMeterFunction;
import com.webcharm.pipeline.functions.EnrichSplitFunction;
import com.webcharm.pipeline.functions.MinioAsyncImageFunction;
import com.webcharm.pipeline.functions.ParseEventFunction;
import com.webcharm.pipeline.functions.PostgresCountAggWriteFunction;
import com.webcharm.pipeline.functions.PostgresImageSizeBucketCountAggWriteFunction;
import com.webcharm.pipeline.functions.PostgresWriteFunction;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.EnrichResult;
import com.webcharm.pipeline.types.EventType;
import com.webcharm.pipeline.types.EventTypeCountAgg;
import com.webcharm.pipeline.types.ImageSizeBucket;
import com.webcharm.pipeline.types.ImageSizeBucketCountAgg;
import com.webcharm.pipeline.types.ProcessedEvent;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

/**
 * Defines and submits the streaming topology. Parses Kafka events, stamps event-time
 * watermarks, then fans out by eventType: IMAGE through async MinIO enrichment, DATA
 * straight to Postgres. Tumbling-window aggregations feed Postgres alongside the raw
 * writes, and every failure from any branch converges on a single dead-letter Kafka topic.
 */
public class StreamingJob {
  private static final Logger log = LoggerFactory.getLogger(StreamingJob.class);

  private static final Duration EVENT_TYPE_COUNT_WINDOW = Duration.ofMinutes(5);
  private static final Duration IMAGE_SIZE_BUCKET_COUNT_WINDOW = Duration.ofMinutes(10);

  /** Runs startup pre-flight checks, then builds the job graph and submits it to the Flink runtime. */
  public static void main(String[] args) throws Exception {
    // Fail fast on a misconfigured deployment before submitting the job.
    PreflightChecks.run();

    String kafkaBootstrap = EnvConfig.env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    String kafkaTopic = EnvConfig.env("KAFKA_TOPIC", "events");
    String dlqTopic = EnvConfig.env("KAFKA_DLQ_TOPIC", "events-dlq");

    StreamExecutionEnvironment executionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    executionEnv.enableCheckpointing(10_000);

    // Retry indefinitely with exponential back-off; let Flink HA keep restarting the job rather
    // than capping attempts and letting it die.
    Configuration restartCfg = new Configuration();
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofSeconds(5));
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofMinutes(10));
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 2.0);
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD,
        Duration.ofMinutes(10));
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR, 0.1);
    executionEnv.configure(restartCfg);

    // Conservative checkpoint tuning for a low-volume local pipeline; these are reasonable starting
    // points, not workload-derived.
    CheckpointConfig ckptConfig = executionEnv.getCheckpointConfig();
    ckptConfig.setMinPauseBetweenCheckpoints(5_000);
    ckptConfig.setCheckpointTimeout(60_000);
    ckptConfig.setMaxConcurrentCheckpoints(1);
    ckptConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(kafkaBootstrap)
        .setTopics(kafkaTopic)
        .setGroupId("flink-processor")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        // Disable Kafka auto-commit; offsets are committed only when a Flink checkpoint succeeds.
        .setProperty("enable.auto.commit", "false")
        .build();

    // No watermarks at the source: records here are still raw JSON strings, so the event-time
    // field buried in the payload can't be read yet. The event-time strategy is assigned one step
    // below, on the parsed ProcessedEvent stream where getEventTime() is available.
    DataStream<String> rawEvents = executionEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-events");

    SingleOutputStreamOperator<ProcessedEvent> parsedEvents = rawEvents
        .process(new ParseEventFunction())
        .name("parse-json");
    DataStream<DlqRecord> parseErrors = parsedEvents.getSideOutput(ParseEventFunction.PARSE_ERROR_TAG);

    DataStream<ProcessedEvent> timedEvents = parsedEvents
        .assignTimestampsAndWatermarks(
            // The watermark only advances when a later event arrives, so the downstream tumbling
            // windows close only while events keep flowing; after the stream goes fully silent the
            // watermark freezes and the trailing window never fires until ingestion resumes.
            WatermarkStrategy
                .<ProcessedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli()))
        .name("event-time-watermarks");

    // Fan out by eventType to the sinks and aggregations; every branch turns its failures into a
    // DlqRecord stream, and the union below funnels them all to the dead-letter topic.
    SingleOutputStreamOperator<ProcessedEvent> imagePipeline = buildImagePipeline(
        timedEvents.filter(e -> EventType.IMAGE.equals(e.getEventType())));
    DataStream<DlqRecord> minioErrors = imagePipeline.getSideOutput(EnrichSplitFunction.UPLOAD_ERROR_TAG);

    DataStream<DlqRecord> imagePostgresErrors = imagePipeline
        .process(new PostgresWriteFunction(DlqStage.IMAGE_POSTGRES))
        .name("image-to-postgres");

    DataStream<DlqRecord> dataPostgresErrors = timedEvents
        .filter(e -> EventType.DATA.equals(e.getEventType()))
        .process(new PostgresWriteFunction(DlqStage.DATA_POSTGRES))
        .name("data-to-postgres");

    DataStream<DlqRecord> countErrors = buildEventTypeCounts(timedEvents)
        .process(new PostgresCountAggWriteFunction(DlqStage.EVENT_TYPE_COUNT_POSTGRES))
        .name("counts-agg-to-postgres");

    DataStream<DlqRecord> imageSizeCountErrors = buildImageSizeBuckets(imagePipeline.getSideOutput(EnrichSplitFunction.IMAGE_SIZE_BUCKET_TAG))
        .process(new PostgresImageSizeBucketCountAggWriteFunction(DlqStage.IMAGE_SIZE_BUCKET_COUNT_POSTGRES))
        .name("image-size-buckets-agg-to-postgres");

    // All dead-letter paths converge on one Kafka producer. The union is type-safe — every input
    // is a DataStream<DlqRecord> — and each record's DlqStage identifies its origin, so one sink
    // serves every branch instead of one sink per branch. That single sink couples DLQ back-pressure
    // across branches, but dead-letter volume is low (only failures), so it is an acceptable trade-off.
    parseErrors
        .union(minioErrors, imagePostgresErrors, dataPostgresErrors, countErrors, imageSizeCountErrors)
        .map(new DlqMeterFunction())
        .name("dlq-meter")
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic))
        .name("dlq-sink");

    log.info("Starting StreamingJob: bootstrap={} topic={}", kafkaBootstrap, kafkaTopic);
    executionEnv.execute("Kafka->Flink->(MinIO,Postgres)");
  }

  /**
   * Counts events per eventType over EVENT_TYPE_COUNT_WINDOW tumbling event-time windows, emitting
   * one EventTypeCountAgg per (type, window) pair when each window closes.
   */
  static DataStream<EventTypeCountAgg> buildEventTypeCounts(DataStream<ProcessedEvent> withWatermarks) {
    return withWatermarks
        .keyBy(ProcessedEvent::getEventType)
        .window(TumblingEventTimeWindows.of(EVENT_TYPE_COUNT_WINDOW))
        // Count events per type over each window.
        .aggregate(
            new CountAggregateFunction<ProcessedEvent>(),
            new ProcessWindowFunction<Long, EventTypeCountAgg, String, TimeWindow>() {
              @Override
              public void process(String key, Context context, Iterable<Long> counts,
                  Collector<EventTypeCountAgg> out) {
                out.collect(new EventTypeCountAgg(
                    Instant.ofEpochMilli(context.window().getStart()),
                    Instant.ofEpochMilli(context.window().getEnd()),
                    key,
                    counts.iterator().next()));
              }
            })
        .name("count-by-type-agg");
  }

  /**
   * Counts stored images per size bucket over IMAGE_SIZE_BUCKET_COUNT_WINDOW tumbling event-time
   * windows, emitting one ImageSizeBucketCountAgg per (bucket, window) pair when each window closes.
   */
  static DataStream<ImageSizeBucketCountAgg> buildImageSizeBuckets(
      DataStream<ImageSizeBucket> sizeBuckets) {
    return sizeBuckets
        // Flink rejects an enum as a key type, so key by the label; the explicit key type is
        // needed because a lambda's return type is not inferable.
        .keyBy(bucket -> bucket.label(), TypeInformation.of(String.class))
        .window(TumblingEventTimeWindows.of(IMAGE_SIZE_BUCKET_COUNT_WINDOW))
        // Count stored images per size bucket over each window.
        .aggregate(
            new CountAggregateFunction<ImageSizeBucket>(),
            new ProcessWindowFunction<Long, ImageSizeBucketCountAgg, String, TimeWindow>() {
              @Override
              public void process(String bucketLabel, Context context, Iterable<Long> counts,
                  Collector<ImageSizeBucketCountAgg> out) {
                out.collect(new ImageSizeBucketCountAgg(
                    Instant.ofEpochMilli(context.window().getStart()),
                    Instant.ofEpochMilli(context.window().getEnd()),
                    bucketLabel,
                    counts.iterator().next()));
              }
            })
        .name("image-size-buckets-agg");
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

    SerializablePredicate<Collection<EnrichResult>> retryable = results -> results.stream().anyMatch(EnrichResult::isRetryable);
    AsyncRetryStrategy<EnrichResult> retryStrategy = new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<EnrichResult>(
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
