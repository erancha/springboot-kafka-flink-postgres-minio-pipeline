package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.config.EnvConfig;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.ProcessedEvent;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessFunction that uploads the image payload to MinIO and replaces the binary fields with the resulting object key.
 * Follows SRP: responsible for MinIO upload only; the Postgres write is handled separately by PostgresProcessedEventSink.
 * Upload and decode failures are routed to UPLOAD_ERROR_TAG (side output) as DlqRecords so a bad event or transient MinIO error does not trigger a Flink job restart and Kafka replay.
 * Binary payload is cleared before any stateful operator to keep checkpoint state small.
 */
public class MinioUploadFunction extends ProcessFunction<ProcessedEvent, ProcessedEvent> {

  /** Side-output tag for events that fail MinIO upload or image decoding. */
  public static final OutputTag<DlqRecord> UPLOAD_ERROR_TAG = new OutputTag<DlqRecord>("minio-upload-error") {
  };

  private static final Logger log = LoggerFactory.getLogger(MinioUploadFunction.class);

  private static final int CONNECT_TIMEOUT_SECS = 10; // TCP connect limit
  private static final int READ_TIMEOUT_SECS = 30; // per-request read limit
  private static final long MAX_IMAGE_BYTES = 10L * 1024 * 1024; // hard cap; prevents OOM on huge responses
  private static final int MAX_FETCH_ATTEMPTS = 3; // total attempts before routing to DLQ

  // Base unit for exponential backoff: sleep = interRetryDelayMs × 2^(attempt-1) (1 s, 2 s, 4 s, …).
  // Set to 0 in the test constructor so retries are instant.
  private final long interRetryDelayMs;

  private transient MinioClient minio;
  private transient HttpClient http;
  private transient ObjectMapper mapper;

  /**
   * Initializes the MinIO client, HTTP client, and JSON serializer once per task slot.
   * Called by Flink before the first processElement invocation on this operator instance.
   */
  @Override
  public void open(OpenContext openContext) {
    String endpoint = EnvConfig.env("MINIO_ENDPOINT", "http://minio:9000");
    String accessKey = EnvConfig.env("MINIO_ACCESS_KEY", "minio");
    String secretKey = EnvConfig.env("MINIO_SECRET_KEY", "minio123");
    this.minio = MinioClient.builder()
        .endpoint(endpoint)
        .credentials(accessKey, secretKey)
        .build();
    this.http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(CONNECT_TIMEOUT_SECS))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    this.mapper = new ObjectMapper();
    log.info("MinioUploadFunction initialized: endpoint={}", endpoint);
  }

  /**
   * Routes the event through upload(). On any exception, emits a DlqRecord to UPLOAD_ERROR_TAG instead of propagating, 
   * thus preventing job restart on transient MinIO errors or malformed payloads.
   */
  @Override
  public void processElement(ProcessedEvent value, Context ctx, Collector<ProcessedEvent> out) {
    try {
      out.collect(upload(value));
    } catch (Exception e) {
      log.error("MinIO upload failed for event id={}: {}", value.getId(), e.getMessage(), e);
      ctx.output(UPLOAD_ERROR_TAG, new DlqRecord(toRawString(value), e.getMessage(), Instant.now()));
    }
  }

  /** Serializes the event to JSON for the DLQ raw field; falls back to the event ID if serialization itself fails. */
  private String toRawString(ProcessedEvent value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      return value.getId().toString();
    }
  }

  /**
   * Fetches or decodes the image, uploads it to MinIO, then returns the same event with the binary fields cleared and imageObjectKey populated. 
   * The object key is derived solely from event fields (id and imageContentType), making it deterministic across replays.
   * For URL-sourced images, a statObject existence check is performed before fetching — if the object is already present the fetch and upload are skipped, 
   * making the URL path idempotent under Flink at-least-once replay.
   *
   * @param value the ProcessedEvent deserialized from the Kafka events topic
   * @return the event with imageBase64 and imageUrl nulled out and imageObjectKey set;
   *         returned unchanged if neither image field is present
   */
  ProcessedEvent upload(ProcessedEvent value) throws Exception {
    String bucket = EnvConfig.env("MINIO_BUCKET", "images");
    String contentType = (value.getImageContentType() == null || value.getImageContentType().isBlank())
        ? "image/jpeg"
        : value.getImageContentType();

    String date = DateTimeFormatter.ISO_LOCAL_DATE.format(value.getDate());
    String extension = guessExtension(contentType);
    String objectKey = "images/" + date + "/" + value.getId() + extension;

    byte[] bytes;
    if (value.getImageBase64() != null && !value.getImageBase64().isBlank()) {
      bytes = Base64.getDecoder().decode(value.getImageBase64());
    } else if (value.getImageUrl() != null && !value.getImageUrl().isBlank()) {
      validateImageUrl(value.getImageUrl());
      if (objectExists(bucket, objectKey)) {
        log.debug("Skipping upload — object already exists: key={}", objectKey);
        value.setImageObjectKey(objectKey);
        value.setImageUrl(null);
        return value;
      }
      bytes = fetchWithRetry(value.getImageUrl());
    } else {
      log.warn("IMAGE event id={} has no imageBase64 or imageUrl — skipping upload", value.getId());
      return value;
    }

    try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
      minio.putObject(
          PutObjectArgs.builder()
              .bucket(bucket).object(objectKey)
              .stream(in, bytes.length, -1)
              .contentType(contentType)
              .build());
    }
    log.debug("Uploaded image: key={}", objectKey);

    value.setImageObjectKey(objectKey);
    value.setImageBase64(null);
    value.setImageUrl(null);
    return value;
  }

  public MinioUploadFunction() {
    this.interRetryDelayMs = 1_000;
  }

  /** Allows injecting clients in unit tests without starting Docker (zero retry delay). */
  MinioUploadFunction(MinioClient minio, HttpClient http) {
    this.minio = minio;
    this.http = http;
    this.mapper = new ObjectMapper();
    this.interRetryDelayMs = 0;
  }

  /**
   * Retries fetch up to MAX_FETCH_ATTEMPTS times on IOException (network error or timeout) or 5xx.
   * Sleep between attempts is exponential: 1s 2s 4s (exponential backoff).
   * Non-retryable failures (4xx, size cap exceeded) propagate immediately to the DLQ path.
   */
  private byte[] fetchWithRetry(String url) throws Exception {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_FETCH_ATTEMPTS; attempt++) {
      try {
        return fetch(url);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      } catch (IOException e) {
        last = e;
        log.warn("Fetch attempt {}/{} failed for url={}: {}", attempt, MAX_FETCH_ATTEMPTS, url, e.getMessage());
        if (attempt < MAX_FETCH_ATTEMPTS && interRetryDelayMs > 0) {
          Thread.sleep(interRetryDelayMs << (attempt - 1)); // 1 s, 2 s, 4 s … (left-shift doubles the delay each attempt)
        }
      }
    }
    throw new IllegalStateException("All " + MAX_FETCH_ATTEMPTS + " fetch attempts failed for url=" + url, last);
  }

  /**
   * Single HTTP fetch with a per-request read timeout and a response-size cap.
   * Throws IOException for transient 5xx (retryable); IllegalStateException for 4xx or
   * oversized response (permanent, routed to DLQ by processElement).
   */
  private byte[] fetch(String url) throws Exception {
    HttpRequest req = HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofSeconds(READ_TIMEOUT_SECS))
        .GET()
        .build();
    HttpResponse<InputStream> resp = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
    int status = resp.statusCode();
    if (status / 100 == 5) {
      throw new IOException("Transient server error fetching imageUrl status=" + status);
    }
    if (status / 100 != 2) {
      throw new IllegalStateException("Failed to fetch imageUrl status=" + status);
    }
    try (InputStream body = resp.body()) {
      byte[] bytes = body.readNBytes((int) (MAX_IMAGE_BYTES + 1));
      if (bytes.length > MAX_IMAGE_BYTES) {
        throw new IllegalStateException(
            "Image response exceeds " + (MAX_IMAGE_BYTES / 1024 / 1024) + " MB cap for url=" + url);
      }
      return bytes;
    }
  }

  /**
   * Returns true if the object already exists in MinIO; false if it does not.
   * Any exception other than NoSuchKey propagates to the caller.
   */
  private boolean objectExists(String bucket, String objectKey) throws Exception {
    try {
      minio.statObject(StatObjectArgs.builder().bucket(bucket).object(objectKey).build());
      return true;
    } catch (ErrorResponseException e) {
      if ("NoSuchKey".equals(e.errorResponse().code())) {
        return false;
      }
      throw e;
    }
  }

  /**
   * Guards against SSRF by rejecting non-http(s) schemes and, when IMAGE_URL_ALLOWED_HOSTS is
   * set, any host not in the comma-separated allowlist.
   *
   * @param rawUrl the imageUrl value from the event
   * @throws IllegalArgumentException if the URL is malformed, uses a disallowed scheme, has no
   *         host, or the host is not in the allowlist
   */
  static void validateImageUrl(String rawUrl) {
    URI uri;
    try {
      uri = URI.create(rawUrl);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("imageUrl is not a valid URI: " + rawUrl, e);
    }
    String scheme = uri.getScheme();
    if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException("imageUrl scheme not allowed: " + scheme);
    }
    String host = uri.getHost();
    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException("imageUrl has no host");
    }
    String allowedHosts = EnvConfig.env("IMAGE_URL_ALLOWED_HOSTS", "");
    if (!allowedHosts.isBlank()) {
      boolean allowed = Arrays.stream(allowedHosts.split(","))
          .map(String::trim)
          .anyMatch(h -> h.equalsIgnoreCase(host));
      if (!allowed) {
        throw new IllegalArgumentException("imageUrl host not in allowlist: " + host);
      }
    }
  }

  /**
   * Maps a MIME content-type string to a file extension. Defaults to .jpg for unrecognised types.
   *
   * @param contentType the MIME type, e.g. "image/png"
   * @return ".png", ".webp", ".gif", or ".jpg"
   */
  private static String guessExtension(String contentType) {
    String ct = contentType == null ? "" : contentType.toLowerCase();
    if (ct.contains("png"))
      return ".png";
    if (ct.contains("webp"))
      return ".webp";
    if (ct.contains("gif"))
      return ".gif";
    return ".jpg";
  }
}
