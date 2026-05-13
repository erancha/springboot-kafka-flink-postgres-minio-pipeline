package com.webcharm.pipeline.functions;

import com.webcharm.pipeline.config.EnvConfig;
import com.webcharm.pipeline.types.ProcessedEvent;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uploads the image payload to MinIO and replaces the binary fields with the resulting object key.
 * Follows SRP: responsible for MinIO upload only; the Postgres write is handled separately by PostgresProcessedEventSink.
 * Previously both operations lived in a single sink class (S3LikeImageSink), making each hard to test or reuse in isolation.
 * Implemented as a map function so the binary payload is cleared before the event reaches any stateful operator,
 * keeping checkpoint state small.
 */
public class MinioUploadFunction extends RichMapFunction<ProcessedEvent, ProcessedEvent> {

  private static final Logger log = LoggerFactory.getLogger(MinioUploadFunction.class);

  private transient MinioClient minio;
  private transient HttpClient http;

  /**
   * Initializes the MinIO client and HTTP client once per task slot.
   * Called by Flink before the first {@link #map} invocation on this operator instance.
   */
  @Override
  public void open(OpenContext openContext) {
    String endpoint  = EnvConfig.env("MINIO_ENDPOINT",   "http://minio:9000");
    String accessKey = EnvConfig.env("MINIO_ACCESS_KEY",  "minio");
    String secretKey = EnvConfig.env("MINIO_SECRET_KEY",  "minio123");
    this.minio = MinioClient.builder()
        .endpoint(endpoint)
        .credentials(accessKey, secretKey)
        .build();
    this.http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
    log.info("MinioUploadFunction initialized: endpoint={}", endpoint);
  }

  /**
   * Transforms the incoming Kafka event: fetches or decodes the image, uploads it to MinIO,
   * then returns the same event with the binary fields cleared and imageObjectKey populated.
   * The object key is derived solely from event fields (id and imageContentType), making it
   * deterministic across replays. For URL-sourced images, a statObject existence check is
   * performed before fetching — if the object is already present the fetch and upload are skipped,
   * making the URL path idempotent under Flink at-least-once replay.
   *
   * @param value the ProcessedEvent deserialized from the Kafka events topic
   * @return the event with imageBase64 and imageUrl nulled out and imageObjectKey set;
   *         returned unchanged if neither image field is present
   */
  @Override
  public ProcessedEvent map(ProcessedEvent value) throws Exception {
    String bucket      = EnvConfig.env("MINIO_BUCKET", "images");
    String contentType = (value.getImageContentType() == null || value.getImageContentType().isBlank())
        ? "image/jpeg" : value.getImageContentType();

    String date      = DateTimeFormatter.ISO_LOCAL_DATE.format(value.getDate());
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
      HttpRequest req = HttpRequest.newBuilder(URI.create(value.getImageUrl())).GET().build();
      HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
      if (resp.statusCode() / 100 != 2) {
        throw new IllegalStateException("Failed to fetch imageUrl status=" + resp.statusCode());
      }
      bytes = resp.body();
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

  public MinioUploadFunction() {}

  /** Allows injecting clients in unit tests without starting Docker. */
  MinioUploadFunction(MinioClient minio, HttpClient http) {
    this.minio = minio;
    this.http  = http;
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
    if (ct.contains("png"))  return ".png";
    if (ct.contains("webp")) return ".webp";
    if (ct.contains("gif"))  return ".gif";
    return ".jpg";
  }
}
