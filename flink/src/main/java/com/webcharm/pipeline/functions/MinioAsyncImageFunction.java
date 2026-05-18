package com.webcharm.pipeline.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.config.EnvConfig;
import com.webcharm.pipeline.types.DlqRecord;
import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.EnrichResult;
import com.webcharm.pipeline.types.ProcessedEvent;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Non-blocking image enrichment as a Flink async I/O function: fetches imageUrl via
 * HttpClient.sendAsync and uploads to MinIO on a bounded executor, never blocking the
 * operator task thread. Emits EnrichResult: success, retryable failure, or permanent failure.
 */
public class MinioAsyncImageFunction extends RichAsyncFunction<ProcessedEvent, EnrichResult> {

  private static final Logger log = LoggerFactory.getLogger(MinioAsyncImageFunction.class);

  private static final long MAX_IMAGE_BYTES = 10L * 1024 * 1024;

  /** Marker for an unrecoverable image failure; everything else is treated as retryable. */
  private static final class PermanentImageException extends RuntimeException {
    PermanentImageException(String message) {
      super(message);
    }
  }

  private transient MinioClient minio;
  private transient HttpClient http;
  private transient ExecutorService ownedExecutor;
  private transient Executor executor;
  private transient ObjectMapper mapper;

  private final int connectTimeoutSecs;
  private final int readTimeoutSecs;
  private final int executorThreads;

  /** No-arg constructor; clients and the executor are built in open(). */
  public MinioAsyncImageFunction() {
    connectTimeoutSecs = EnvConfig.envInt("MINIO_FETCH_CONNECT_TIMEOUT_SECS", 10);
    readTimeoutSecs = EnvConfig.envInt("MINIO_FETCH_READ_TIMEOUT_SECS", 30);
    executorThreads = EnvConfig.envInt("MINIO_ASYNC_EXECUTOR_THREADS", 32);
  }

  /** Injects pre-built clients and an executor, bypassing open(). */
  MinioAsyncImageFunction(MinioClient minio, HttpClient http, Executor executor) {
    this();
    this.minio = minio;
    this.http = http;
    this.executor = executor;
    mapper = new ObjectMapper();
  }

  /** Builds the MinIO client, redirect-blocking HTTP client, executor, and JSON mapper once per slot. */
  @Override
  public void open(OpenContext openContext) {
    if (minio == null) {
      minio = MinioClient.builder()
          .endpoint(EnvConfig.env("MINIO_ENDPOINT", "http://minio:9000"))
          .credentials(EnvConfig.env("MINIO_ACCESS_KEY", "minio"),
              EnvConfig.env("MINIO_SECRET_KEY", "minio123"))
          .build();
    }
    if (http == null) {
      // followRedirects(NEVER) enforces the SSRF control: a 3xx from an allowlisted host
      // cannot redirect the socket to an internal endpoint; a 3xx falls through to the non-2xx guard.
      http = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(connectTimeoutSecs))
          .followRedirects(HttpClient.Redirect.NEVER)
          .build();
    }
    if (executor == null) {
      ownedExecutor = Executors.newFixedThreadPool(executorThreads);
      executor = ownedExecutor;
    }
    if (mapper == null) {
      mapper = new ObjectMapper();
    }
    log.info("MinioAsyncImageFunction initialized: executorThreads={}", executorThreads);
  }

  /** Submits enrichment without blocking the task thread; completes the ResultFuture from a callback. */
  @Override
  public void asyncInvoke(ProcessedEvent value, ResultFuture<EnrichResult> resultFuture) {
    enrich(value).whenComplete((result, err) -> {
      if (err != null) {
        resultFuture.complete(Collections.singleton(classify(value, err)));
      } else {
        resultFuture.complete(Collections.singleton(result));
      }
    });
  }

  /** On timeout, completes with a permanent (non-retryable) failure result. */
  @Override
  public void timeout(ProcessedEvent value, ResultFuture<EnrichResult> resultFuture) {
    log.warn("Async enrichment timed out for event id={}", value.getId());
    resultFuture.complete(Collections.singleton(
        EnrichResult.permanentFailure(
            new DlqRecord(DlqStage.IMAGE_ENRICH, toRawString(value), "async enrichment timed out", Instant.now()))));
  }

  /**
   * Builds the enrichment future. Passthrough and idempotency guard short-circuit; otherwise
   * fetch (async HTTP) then upload (blocking MinIO on the executor). Never blocks, and always
   * completes with a classified EnrichResult (success, permanent, or retryable failure),
   * never exceptionally.
   */
  CompletableFuture<EnrichResult> enrich(ProcessedEvent value) {
    if (value.getImageObjectKey() != null) {
      return CompletableFuture.completedFuture(EnrichResult.success(value));
    }
    String url = value.getImageUrl();
    if (url == null || url.isBlank()) {
      return CompletableFuture.completedFuture(EnrichResult.permanentFailure(
          new DlqRecord(DlqStage.IMAGE_ENRICH, toRawString(value),
              "IMAGE event has neither imageUrl nor imageObjectKey", Instant.now())));
    }

    String bucket = EnvConfig.env("MINIO_BUCKET", "images");
    String date = DateTimeFormatter.ISO_LOCAL_DATE.format(value.getDate());
    String extension = guessExtensionFromUrl(url);
    String objectKey = "images/" + date + "/" + value.getId() + extension;
    String contentType = guessContentType(extension);

    return CompletableFuture
        .supplyAsync(() -> objectExistsUnchecked(bucket, objectKey), executor)
        .thenCompose(exists -> exists
            ? CompletableFuture.completedFuture(EnrichResult.success(withObjectKey(value, objectKey)))
            : fetch(url).thenComposeAsync(bytes -> {
                putObjectUnchecked(bucket, objectKey, bytes, contentType);
                return CompletableFuture.completedFuture(
                    EnrichResult.success(withObjectKey(value, objectKey)));
              }, executor))
        .handle((res, err) -> err == null ? res : classify(value, err));
  }

  /**
   * Single async HTTP GET with read timeout, status classification, and the 10 MB cap.
   * sendAsync resolves its future when the response headers arrive while the body is still
   * streaming, so the blocking readNBytes runs via thenApplyAsync on the bounded executor
   * rather than on the HttpClient's own thread, keeping every external I/O path bounded.
   */
  private CompletableFuture<byte[]> fetch(String url) {
    HttpRequest req = HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofSeconds(readTimeoutSecs))
        .GET()
        .build();
    return http.sendAsync(req, HttpResponse.BodyHandlers.ofInputStream())
        .thenApplyAsync(resp -> {
          int status = resp.statusCode();
          if (status / 100 == 5) {
            throw new RuntimeException("Transient server error status=" + status);
          }
          if (status / 100 != 2) {
            // covers 3xx (redirect/SSRF guard) and 4xx
            throw new PermanentImageException("Non-2xx fetching imageUrl status=" + status);
          }
          try (InputStream body = resp.body()) {
            byte[] bytes = body.readNBytes((int) (MAX_IMAGE_BYTES + 1));
            if (bytes.length > MAX_IMAGE_BYTES) {
              throw new PermanentImageException(
                  "Image response exceeds " + (MAX_IMAGE_BYTES / 1024 / 1024) + " MB cap");
            }
            return bytes;
          } catch (java.io.IOException e) {
            throw new RuntimeException("Error reading image body", e);
          }
        }, executor);
  }

  /** Maps a completion error to permanent (PermanentImageException) or retryable (everything else). */
  private EnrichResult classify(ProcessedEvent value, Throwable err) {
    Throwable cause = unwrap(err);
    String msg = cause.getMessage() == null ? cause.toString() : cause.getMessage();
    DlqRecord record = new DlqRecord(DlqStage.IMAGE_ENRICH, toRawString(value), msg, Instant.now());
    if (cause instanceof PermanentImageException) {
      log.warn("Permanent image failure for id={}: {}", value.getId(), msg);
      return EnrichResult.permanentFailure(record);
    }
    log.warn("Retryable image failure for id={}: {}", value.getId(), msg);
    return EnrichResult.retryableFailure(record);
  }

  private static Throwable unwrap(Throwable t) {
    Throwable c = t;
    while ((c instanceof java.util.concurrent.CompletionException
        || c instanceof java.util.concurrent.ExecutionException) && c.getCause() != null) {
      c = c.getCause();
    }
    return c;
  }

  /** statObject existence guard; NoSuchKey means absent, any other error is rethrown (retryable). */
  private boolean objectExistsUnchecked(String bucket, String objectKey) {
    try {
      minio.statObject(StatObjectArgs.builder().bucket(bucket).object(objectKey).build());
      return true;
    } catch (ErrorResponseException e) {
      if ("NoSuchKey".equals(e.errorResponse().code())) {
        return false;
      }
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Uploads the fetched bytes to MinIO; wraps checked exceptions so they classify as retryable. */
  private void putObjectUnchecked(String bucket, String objectKey, byte[] bytes, String contentType) {
    try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
      minio.putObject(PutObjectArgs.builder()
          .bucket(bucket).object(objectKey)
          .stream(in, bytes.length, -1)
          .contentType(contentType)
          .build());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns a new event with imageObjectKey set and imageUrl cleared; never mutates the input. */
  private static ProcessedEvent withObjectKey(ProcessedEvent src, String objectKey) {
    return new ProcessedEvent(src.getId(), src.getEventType(), src.getEventTime(), src.getSource(),
        src.getPayload(), null, objectKey, src.getDate());
  }

  /** Serializes the event for the DLQ raw field; falls back to the id when serialization fails. */
  private String toRawString(ProcessedEvent value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      return value.getId().toString();
    }
  }

  /** Derives a file extension from the URL path; defaults to .jpg for unrecognized extensions. */
  private static String guessExtensionFromUrl(String url) {
    try {
      String path = URI.create(url).getPath().toLowerCase();
      if (path.endsWith(".png")) return ".png";
      if (path.endsWith(".webp")) return ".webp";
      if (path.endsWith(".gif")) return ".gif";
    } catch (Exception ignored) {
    }
    return ".jpg";
  }

  /** Maps a file extension to its MIME content-type; defaults to image/jpeg. */
  private static String guessContentType(String extension) {
    return switch (extension) {
      case ".png" -> "image/png";
      case ".webp" -> "image/webp";
      case ".gif" -> "image/gif";
      default -> "image/jpeg";
    };
  }

  /** Shuts down the owned executor (if any) on operator close. */
  @Override
  public void close() {
    if (ownedExecutor != null) {
      ownedExecutor.shutdownNow();
    }
  }
}
