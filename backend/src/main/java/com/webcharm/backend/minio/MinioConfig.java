package com.webcharm.backend.minio;

import io.minio.MinioClient;
import io.minio.http.HttpUtils;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Produces the MinioClient Spring bean from application.yml minio.* properties, backed by an
 * HTTP client whose connect, write, and read timeouts are explicitly bounded so a degraded
 * MinIO cannot hold a Tomcat request thread for the minio SDK's 5-minute default and
 * exhaust the worker pool.
 */
@Configuration
class MinioConfig {

  @Value("${minio.endpoint}")
  private String endpoint;

  @Value("${minio.access-key}")
  private String accessKey;

  @Value("${minio.secret-key}")
  private String secretKey;

  @Value("${minio.connect-timeout-secs}")
  private int connectTimeoutSecs;

  @Value("${minio.write-timeout-secs}")
  private int writeTimeoutSecs;

  @Value("${minio.read-timeout-secs}")
  private int readTimeoutSecs;

  @Bean
  MinioClient minioClient() {
    return MinioClient.builder()
        .endpoint(endpoint)
        .credentials(accessKey, secretKey)
        .httpClient(httpClient(connectTimeoutSecs, writeTimeoutSecs, readTimeoutSecs))
        .build();
  }

  /**
   * Builds the OkHttpClient backing the MinioClient with the given connect, write, and read
   * timeouts in seconds. The minio SDK otherwise defaults every timeout to 5 minutes, which
   * would let a slow MinIO block the calling request thread for that long with no app-level
   * cap.
   */
  static OkHttpClient httpClient(int connectTimeoutSecs, int writeTimeoutSecs,
      int readTimeoutSecs) {
    return HttpUtils.newDefaultHttpClient(
        connectTimeoutSecs * 1000L, writeTimeoutSecs * 1000L, readTimeoutSecs * 1000L);
  }
}
