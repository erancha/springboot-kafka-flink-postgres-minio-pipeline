package com.webcharm.backend.minio;

import static org.junit.jupiter.api.Assertions.assertEquals;

import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;

/** Verifies the MinIO HTTP client is built with the configured bounded timeouts. */
class MinioConfigTest {

  @Test
  void httpClient_appliesConfiguredTimeouts_notSdkFiveMinuteDefault() {
    OkHttpClient client = MinioConfig.httpClient(2, 3, 4);

    assertEquals(2_000, client.connectTimeoutMillis());
    assertEquals(3_000, client.writeTimeoutMillis());
    assertEquals(4_000, client.readTimeoutMillis());
  }
}
