package com.webcharm.pipeline.common.config;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

/**
 * Verifies JobEnvironment.configureFaultTolerance applies the shared checkpointing and
 * exponential-restart settings both pipelines rely on, so neither pipeline re-declares them.
 */
class JobEnvironmentTest {

  @Test
  void appliesCheckpointConfig() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    JobEnvironment.configureFaultTolerance(env);

    CheckpointConfig ckpt = env.getCheckpointConfig();
    assertTrue(ckpt.isCheckpointingEnabled());
    assertEquals(10_000, ckpt.getCheckpointInterval());
    assertEquals(5_000, ckpt.getMinPauseBetweenCheckpoints());
    assertEquals(60_000, ckpt.getCheckpointTimeout());
    assertEquals(1, ckpt.getMaxConcurrentCheckpoints());
    assertEquals(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION,
        ckpt.getExternalizedCheckpointRetention());
  }

  @Test
  void appliesExponentialRestartStrategy() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    JobEnvironment.configureFaultTolerance(env);

    ReadableConfig cfg = env.getConfiguration();
    assertEquals("exponential-delay", cfg.get(RestartStrategyOptions.RESTART_STRATEGY));
    assertEquals(Duration.ofSeconds(5),
        cfg.get(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF));
    assertEquals(Duration.ofMinutes(10),
        cfg.get(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF));
    assertEquals(2.0,
        cfg.get(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER));
    assertEquals(Duration.ofMinutes(10),
        cfg.get(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD));
    assertEquals(0.1,
        cfg.get(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR));
  }
}
