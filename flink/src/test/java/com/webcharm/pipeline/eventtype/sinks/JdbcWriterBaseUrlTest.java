package com.webcharm.pipeline.eventtype.sinks;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Verifies withTimeoutParams appends socketTimeout and connectTimeout independently,
 * never dropping one because the other (or an unrelated query param) is already present.
 */
class JdbcWriterBaseUrlTest {

  @Test
  void noParams_appendsBothWithQuestionMark() {
    String out = JdbcWriterBase.withTimeoutParams("jdbc:postgresql://h:5432/db");
    assertEquals("jdbc:postgresql://h:5432/db?socketTimeout=20&connectTimeout=5", out);
  }

  @Test
  void existingUnrelatedParam_appendsBothWithAmpersand() {
    String out = JdbcWriterBase.withTimeoutParams("jdbc:postgresql://h:5432/db?ssl=true");
    assertEquals("jdbc:postgresql://h:5432/db?ssl=true&socketTimeout=20&connectTimeout=5", out);
  }

  @Test
  void onlyConnectTimeoutPresent_stillAppendsSocketTimeout() {
    String out = JdbcWriterBase.withTimeoutParams("jdbc:postgresql://h:5432/db?connectTimeout=3");
    assertEquals("jdbc:postgresql://h:5432/db?connectTimeout=3&socketTimeout=20", out);
  }

  @Test
  void onlySocketTimeoutPresent_stillAppendsConnectTimeout() {
    String out = JdbcWriterBase.withTimeoutParams("jdbc:postgresql://h:5432/db?socketTimeout=2");
    assertEquals("jdbc:postgresql://h:5432/db?socketTimeout=2&connectTimeout=5", out);
  }

  @Test
  void bothPresent_returnsUrlUnchanged() {
    String url = "jdbc:postgresql://h:5432/db?socketTimeout=2&connectTimeout=3";
    assertEquals(url, JdbcWriterBase.withTimeoutParams(url));
  }
}
