package com.webcharm.backend.objectstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;

/** Verifies the object-store HTTP client is built with the configured bounded timeouts. */
class ObjectStoreConfigTest {

  @Test
  void httpClient_appliesConfiguredTimeouts_notSdkFiveMinuteDefault() {
    OkHttpClient client = ObjectStoreConfig.httpClient(2, 3, 4);

    assertEquals(2_000, client.connectTimeoutMillis());
    assertEquals(3_000, client.writeTimeoutMillis());
    assertEquals(4_000, client.readTimeoutMillis());
  }
}
