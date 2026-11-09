package com.webcharm.backend.objectstore;

import io.minio.MinioClient;
import io.minio.http.HttpUtils;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Produces the MinioClient Spring bean from application.yml object-store.* properties, backed by an
 * HTTP client whose connect, write, and read timeouts are explicitly bounded so a degraded
 * object store cannot hold a Tomcat request thread for the MinIO client library's 5-minute
 * default and exhaust the worker pool.
 */
@Configuration
class ObjectStoreConfig {

  @Value("${object-store.endpoint}")
  private String endpoint;

  @Value("${object-store.access-key}")
  private String accessKey;

  @Value("${object-store.secret-key}")
  private String secretKey;

  @Value("${object-store.connect-timeout-secs}")
  private int connectTimeoutSecs;

  @Value("${object-store.write-timeout-secs}")
  private int writeTimeoutSecs;

  @Value("${object-store.read-timeout-secs}")
  private int readTimeoutSecs;

  @Bean
  MinioClient objectStoreClient() {
    return MinioClient.builder()
        .endpoint(endpoint)
        .credentials(accessKey, secretKey)
        .httpClient(httpClient(connectTimeoutSecs, writeTimeoutSecs, readTimeoutSecs))
        .build();
  }

  static OkHttpClient httpClient(int connectTimeoutSecs, int writeTimeoutSecs,
      int readTimeoutSecs) {
    return HttpUtils.newDefaultHttpClient(
        connectTimeoutSecs * 1000L, writeTimeoutSecs * 1000L, readTimeoutSecs * 1000L);
  }
}
