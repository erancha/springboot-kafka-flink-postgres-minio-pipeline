package com.webcharm.pipeline.common.config;

import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Shared fault-tolerance configuration applied identically to every pipeline's execution environment. */
public final class JobEnvironment {
  private JobEnvironment() {}

  /**
   * Enables checkpointing, an indefinite exponential-backoff restart strategy, and conservative
   * checkpoint tuning on the given environment.
   *
   * The restart strategy never caps attempts, so Flink HA keeps restarting the job rather than
   * letting it die. maxConcurrentCheckpoints is 1, which keeps at most one checkpoint in flight per
   * subtask; for an exactly-once XA JDBC sink that also bounds the in-flight prepared transactions,
   * and thus the Postgres max_prepared_transactions the deployment must provision. The remaining
   * values are conservative starting points for a low-volume pipeline, not workload-derived.
   */
  public static void configureFaultTolerance(StreamExecutionEnvironment env) {
    env.enableCheckpointing(10_000);

    Configuration restartCfg = new Configuration();
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofSeconds(5));
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofMinutes(10));
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 2.0);
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD,
        Duration.ofMinutes(10));
    restartCfg.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR, 0.1);
    env.configure(restartCfg);

    CheckpointConfig ckptConfig = env.getCheckpointConfig();
    ckptConfig.setMinPauseBetweenCheckpoints(5_000);
    ckptConfig.setCheckpointTimeout(60_000);
    ckptConfig.setMaxConcurrentCheckpoints(1);
    ckptConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
  }
}
