package com.webcharm.pipeline.userkeys;

import com.webcharm.pipeline.common.config.EnvConfig;
import com.webcharm.pipeline.common.dlq.DlqMeterFunction;
import com.webcharm.pipeline.common.dlq.DlqRecord;
import com.webcharm.pipeline.common.dlq.DlqRecordSerializer;
import com.webcharm.pipeline.userkeys.functions.ParseUserKeyFunction;
import com.webcharm.pipeline.userkeys.functions.SumAggregateFunction;
import com.webcharm.pipeline.userkeys.types.DlqStage;
import com.webcharm.pipeline.userkeys.types.UserKeyAgg;
import com.webcharm.pipeline.userkeys.types.UserKeyEvent;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import javax.sql.XADataSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
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
import org.apache.flink.util.function.SerializableSupplier;
import org.postgresql.xa.PGXADataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines and submits the userKeys streaming topology. Parses Kafka events, stamps event-time
 * watermarks, sums value over tumbling windows keyed by (userId, key), and writes each window's
 * result to Postgres through an exactly-once XA sink. Malformed events are diverted to a dead-letter
 * Kafka topic at the parse stage, before windowing.
 *
 * Exactly-once into Postgres comes from Flink's own delivery, not from idempotent writes: the JDBC
 * sink prepares an XA transaction at each checkpoint and commits it only when the checkpoint
 * completes, so each window result is committed once and the INSERT needs no upsert.
 */
public class StreamingJob {
  private static final Logger log = LoggerFactory.getLogger(StreamingJob.class);

  private static final String AGGREGATE_INSERT_SQL =
      "INSERT INTO user_key_aggregates (window_start, window_end, user_id, key, value_sum) "
          + "VALUES (?, ?, ?, ?, ?)";

  /** Runs startup pre-flight checks before building and submitting the job graph. */
  public static void main(String[] args) throws Exception {
    // Fail fast on a misconfigured deployment before submitting the job.
    PreflightChecks.run();

    String kafkaBootstrap = EnvConfig.env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    String kafkaTopic = EnvConfig.env("KAFKA_USERKEYS_TOPIC", "user-keys");
    String dlqTopic = EnvConfig.env("KAFKA_USERKEYS_DLQ_TOPIC", "user-keys-dlq");
    Duration window = Duration.ofSeconds(EnvConfig.envInt("USERKEYS_WINDOW_SECONDS", 60));

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

    // The XA sink prepares one transaction per checkpoint and commits it on completion, so
    // maxConcurrentCheckpoints stays 1: at most one prepared transaction is in flight per sink
    // subtask, which bounds the Postgres max_prepared_transactions the deployment must provision.
    CheckpointConfig ckptConfig = executionEnv.getCheckpointConfig();
    ckptConfig.setMinPauseBetweenCheckpoints(5_000);
    ckptConfig.setCheckpointTimeout(60_000);
    ckptConfig.setMaxConcurrentCheckpoints(1);
    ckptConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(kafkaBootstrap)
        .setTopics(kafkaTopic)
        .setGroupId("flink-userkeys-processor")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        // Disable Kafka auto-commit; offsets are committed only when a Flink checkpoint succeeds.
        .setProperty("enable.auto.commit", "false")
        .build();

    // No watermarks at the source: records here are still raw JSON strings, so the event-time
    // field can't be read yet. The strategy is assigned below on the parsed UserKeyEvent stream.
    DataStream<String> rawEvents =
        executionEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-user-keys");

    SingleOutputStreamOperator<UserKeyEvent> parsed = rawEvents
        .process(new ParseUserKeyFunction())
        .name("parse-json");
    DataStream<DlqRecord> parseErrors = parsed.getSideOutput(ParseUserKeyFunction.PARSE_ERROR_TAG);

    DataStream<UserKeyEvent> timed = parsed
        .assignTimestampsAndWatermarks(
            // The operator's watermark is the minimum across its parallel subtasks, so a single
            // subtask that never sees data holds it at -infinity and no window ever closes. When the
            // topic has more partitions than currently-active keys, most subtasks are idle, so
            // withIdleness drops a quiet subtask from that minimum until it receives data again. The
            // window still only advances while some partition keeps producing events.
            WatermarkStrategy
                .<UserKeyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withIdleness(Duration.ofSeconds(15))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli()))
        .name("event-time-watermarks");

    timed
        .keyBy(e -> Tuple2.of(e.getUserId(), e.getKey()), Types.TUPLE(Types.STRING, Types.STRING))
        .window(TumblingEventTimeWindows.of(window))
        .aggregate(new SumAggregateFunction(), new EmitAggregate())
        .name("sum-by-user-key-agg")
        .sinkTo(buildAggregateSink())
        .name("user-key-agg-to-postgres");

    parseErrors
        .map(new DlqMeterFunction<>(DlqStage.class))
        .name("dlq-meter")
        .sinkTo(buildDlqSink(kafkaBootstrap, dlqTopic))
        .name("dlq-sink");

    log.info("Starting userKeys StreamingJob: bootstrap={} topic={} windowSecs={}",
        kafkaBootstrap, kafkaTopic, window.toSeconds());
    executionEnv.execute("Kafka->Flink->Postgres (userKeys)");
  }

  /**
   * Emits one UserKeyAgg per (userId, key) window from the incremental sum, stamped with the
   * window bounds.
   */
  static final class EmitAggregate
      extends ProcessWindowFunction<Long, UserKeyAgg, Tuple2<String, String>, TimeWindow> {
    @Override
    public void process(Tuple2<String, String> key, Context context, Iterable<Long> sums,
        Collector<UserKeyAgg> out) {
      out.collect(new UserKeyAgg(
          Instant.ofEpochMilli(context.window().getStart()),
          Instant.ofEpochMilli(context.window().getEnd()),
          key.f0,
          key.f1,
          sums.iterator().next()));
    }
  }

  /** Resolves the userKeys Postgres connection and timeouts from the environment for the aggregate sink. */
  private static JdbcSink<UserKeyAgg> buildAggregateSink() {
    return aggregateSink(
        EnvConfig.env("POSTGRES_USERKEYS_URL", "jdbc:postgresql://postgres:5432/userkeys"),
        EnvConfig.env("POSTGRES_USER", "postgres"),
        EnvConfig.env("POSTGRES_PASSWORD", "postgres"),
        EnvConfig.envInt("JDBC_SOCKET_TIMEOUT_SECS", 20),
        EnvConfig.envInt("JDBC_CONNECT_TIMEOUT_SECS", 5));
  }

  /**
   * Builds the exactly-once XA sink for window aggregates against the given Postgres connection.
   * maxRetries is 0 because an in-sink retry would break exactly-once; transactionPerConnection is
   * true because PostgreSQL holds at most one XA transaction per connection. Bounded socket and
   * connect timeouts keep a hung Postgres socket from stalling a checkpoint past its timeout.
   * Package-visible so integration tests can target a Testcontainers database.
   */
  static JdbcSink<UserKeyAgg> aggregateSink(String url, String user, String password,
      int socketTimeoutSecs, int connectTimeoutSecs) {
    JdbcStatementBuilder<UserKeyAgg> statementBuilder = (ps, agg) -> {
      ps.setObject(1, agg.getWindowStart().atOffset(ZoneOffset.UTC));
      ps.setObject(2, agg.getWindowEnd().atOffset(ZoneOffset.UTC));
      ps.setString(3, agg.getUserId());
      ps.setString(4, agg.getKey());
      ps.setLong(5, agg.getValueSum());
    };

    SerializableSupplier<XADataSource> xaDataSource = () -> {
      PGXADataSource ds = new PGXADataSource();
      ds.setUrl(url);
      ds.setUser(user);
      ds.setPassword(password);
      ds.setSocketTimeout(socketTimeoutSecs);
      ds.setConnectTimeout(connectTimeoutSecs);
      return ds;
    };

    return JdbcSink.<UserKeyAgg>builder()
        .withQueryStatement(AGGREGATE_INSERT_SQL, statementBuilder)
        .withExecutionOptions(JdbcExecutionOptions.builder().withMaxRetries(0).build())
        .buildExactlyOnce(
            JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build(),
            xaDataSource);
  }

  /**
   * Builds a KafkaSink that writes DlqRecords to the given topic with AT_LEAST_ONCE delivery.
   * AT_LEAST_ONCE flushes pending records at each Flink checkpoint, so a task restart after emitting
   * a DLQ record but before the checkpoint cannot silently drop it. Duplicates are acceptable on the
   * DLQ; silent loss is not.
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
}
