package com.webcharm.pipeline.userkeys;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.webcharm.pipeline.userkeys.functions.SumAggregateFunction;
import com.webcharm.pipeline.userkeys.types.UserKeyEvent;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies the userKeys pipeline's windowed sum and its exactly-once XA sink end to end against a
 * real PostgreSQL managed by Testcontainers. A bounded source emits a final watermark that closes
 * the event-time windows, the XA sink commits each window result on the resulting checkpoint, and
 * the rows are asserted against the expected per-(userId, key) sums.
 *
 * Exactly-once is load-bearing here: the sink does a plain INSERT (no upsert) keyed by
 * (window_start, user_id, key), so a double-commit would violate the primary key and fail the job.
 * A clean run with exactly the expected rows is therefore evidence each window committed once.
 */
@Testcontainers
class UserKeyAggregationIT {

  @Container
  @SuppressWarnings("resource") // lifecycle managed by the @Testcontainers JUnit extension
  static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16")
      .withDatabaseName("userkeys")
      .withUsername("postgres")
      .withPassword("postgres")
      // The XA sink relies on PostgreSQL prepared transactions, which default to disabled (0).
      .withCommand("postgres", "-c", "max_prepared_transactions=10");

  @BeforeAll
  static void createSchema() throws Exception {
    try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), "postgres", "postgres");
         Statement st = c.createStatement()) {
      st.execute("""
          CREATE TABLE user_key_aggregates (
            window_start TIMESTAMPTZ NOT NULL,
            window_end   TIMESTAMPTZ NOT NULL,
            user_id      TEXT        NOT NULL,
            key          TEXT        NOT NULL,
            value_sum    BIGINT      NOT NULL,
            PRIMARY KEY (window_start, user_id, key)
          )
          """);
    }
  }

  @Test
  void windowedSums_committedExactlyOnceToPostgres() throws Exception {
    Instant t = Instant.parse("2024-01-15T10:00:00Z");
    List<UserKeyEvent> events = List.of(
        new UserKeyEvent(UUID.randomUUID(), "user-1", "A", 100, t),
        new UserKeyEvent(UUID.randomUUID(), "user-1", "A", 200, t),
        new UserKeyEvent(UUID.randomUUID(), "user-1", "B", 500, t),
        new UserKeyEvent(UUID.randomUUID(), "user-2", "A", 1000, t));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(500);

    DataStream<UserKeyEvent> source = env
        .fromData(events)
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<UserKeyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((e, ts) -> e.getEventTime().toEpochMilli()));

    source
        .keyBy(e -> Tuple2.of(e.getUserId(), e.getKey()), Types.TUPLE(Types.STRING, Types.STRING))
        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
        .aggregate(new SumAggregateFunction(), new StreamingJob.EmitAggregate())
        .sinkTo(StreamingJob.aggregateSink(pg.getJdbcUrl(), "postgres", "postgres", 20, 5));

    env.execute("userkeys-it");

    Map<String, Long> sums = new HashMap<>();
    int rowCount = 0;
    try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), "postgres", "postgres");
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT user_id, key, value_sum FROM user_key_aggregates")) {
      while (rs.next()) {
        rowCount++;
        sums.put(rs.getString("user_id") + "|" + rs.getString("key"), rs.getLong("value_sum"));
      }
    }

    assertEquals(3, rowCount, "one row per (userId, key) in the single window");
    assertEquals(300L, sums.get("user-1|A"));
    assertEquals(500L, sums.get("user-1|B"));
    assertEquals(1000L, sums.get("user-2|A"));
  }

  @Test
  void schemaPrimaryKey_rejectsDuplicateWindowRow() throws Exception {
    // A direct guard on the invariant the exactly-once sink depends on: a second row for the same
    // (window_start, user_id, key) is impossible, so a double-commit could never silently double-count.
    String insert = "INSERT INTO user_key_aggregates (window_start, window_end, user_id, key, value_sum) "
        + "VALUES ('2030-01-01T00:00:00Z', '2030-01-01T00:01:00Z', 'dup', 'A', 1)";
    boolean rejected = false;
    try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), "postgres", "postgres");
         Statement st = c.createStatement()) {
      st.execute(insert);
      try {
        st.execute(insert);
      } catch (Exception e) {
        rejected = true;
      } finally {
        st.execute("DELETE FROM user_key_aggregates WHERE user_id = 'dup'");
      }
    }
    assertTrue(rejected, "duplicate (window_start, user_id, key) must violate the primary key");
  }
}
