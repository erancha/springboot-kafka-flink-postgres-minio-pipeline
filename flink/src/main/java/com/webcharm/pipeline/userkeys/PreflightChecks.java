package com.webcharm.pipeline.userkeys;

import com.webcharm.pipeline.common.config.EnvConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup validation of external dependencies. Fails fast with a clear message when the userKeys
 * Postgres database is misconfigured (bad credentials, missing table or column) so a broken
 * deployment never starts and thrashes the running pipeline.
 */
public final class PreflightChecks {

  private static final Logger log = LoggerFactory.getLogger(PreflightChecks.class);

  private PreflightChecks() {}

  /**
   * Runs every dependency check against the configured environment. Throws IllegalStateException
   * naming the failing component and cause if any check fails.
   */
  public static void run() {
    String url = EnvConfig.env("POSTGRES_USERKEYS_URL", "jdbc:postgresql://postgres:5432/userkeys");
    String user = EnvConfig.env("POSTGRES_USER", "postgres");
    String password = EnvConfig.env("POSTGRES_PASSWORD", "postgres");
    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      verifyPostgresSchema(conn);
    } catch (Exception e) {
      throw fail("Postgres", e);
    }

    log.info("Pre-flight checks passed: userKeys Postgres schema is reachable");
  }

  /**
   * Verifies the aggregate table exposes the columns the sink binds, so schema drift fails the
   * deployment at startup instead of restart-looping every commit. Throws if the table or any
   * column is missing.
   */
  static void verifyPostgresSchema(Connection conn) throws Exception {
    try (Statement st = conn.createStatement()) {
      st.execute("SELECT window_start, window_end, user_id, key, value_sum "
          + "FROM user_key_aggregates WHERE false");
    }
  }

  private static IllegalStateException fail(String component, Throwable cause) {
    return new IllegalStateException(
        "Pre-flight check failed against " + component + ": " + cause.getMessage(), cause);
  }
}
