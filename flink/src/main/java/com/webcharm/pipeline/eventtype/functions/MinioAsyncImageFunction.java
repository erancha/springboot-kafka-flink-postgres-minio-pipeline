package com.webcharm.pipeline.eventtype.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.pipeline.common.config.EnvConfig;
import com.webcharm.pipeline.common.dlq.DlqRecord;
import com.webcharm.pipeline.eventtype.types.DlqStage;
import com.webcharm.pipeline.eventtype.types.EnrichResult;
import com.webcharm.pipeline.eventtype.types.ProcessedEvent;
import com.webcharm.contract.eventtype.image.ImageFormat;
import com.webcharm.contract.eventtype.image.ImageObjectKey;
import com.webcharm.contract.eventtype.image.ImageRules;
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
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
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
  // Counts retryable enrichment failures so retry pressure on the IMAGE branch is observable in
  // Prometheus. Initialized to a standalone counter for the test/inject path; replaced in open()
  // with the metric-group-registered counter on the cluster.
  private transient Counter retryableFailures = new SimpleCounter();
  // Throughput/performance metrics for the IMAGE branch: number of successful MinIO uploads and
  // cumulative putObject duration in nanoseconds. Average upload latency is derived in Prometheus
  // as rate(minio_upload_nanos) / rate(minio_uploads). Same standalone-then-registered lifecycle
  // as retryableFailures.
  private transient Counter uploadCount = new SimpleCounter();
  private transient Counter uploadNanos = new SimpleCounter();

  private final int connectTimeoutSecs;
  private final int readTimeoutSecs;
  private final int executorThreads;

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

  /**
   * Builds the MinIO client, redirect-blocking HTTP client, executor, and JSON mapper once per
   * slot, and registers the retryable-failure and upload throughput/latency counters with the
   * operator metric group.
   */
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
    retryableFailures = getRuntimeContext().getMetricGroup().counter("image_retryable_failures");
    uploadCount = getRuntimeContext().getMetricGroup().counter("minio_uploads");
    uploadNanos = getRuntimeContext().getMetricGroup().counter("minio_upload_nanos");
    log.info("MinioAsyncImageFunction initialized: executorThreads={}", executorThreads);
  }

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
            new DlqRecord(DlqStage.IMAGE_ENRICH.name(), toRawString(value), "async enrichment timed out", Instant.now()))));
  }

  /**
   * Builds the enrichment future. Passthrough and idempotency guard short-circuit; otherwise
   * fetch (async HTTP) then upload (blocking MinIO on the executor). Never blocks, and always
   * completes with a classified EnrichResult (success, permanent, or retryable failure),
   * never exceptionally.
   */
  CompletableFuture<EnrichResult> enrich(ProcessedEvent value) {
    String bucket = EnvConfig.env("MINIO_BUCKET", "images");
    if (value.getImageObjectKey() != null) {
      // Backend already stored the bytes; read the size best-effort so a stat failure never fails this passthrough.
      return CompletableFuture.supplyAsync(
          () -> EnrichResult.success(value, statSizeBestEffort(bucket, value.getImageObjectKey())),
          executor);
    }
    String url = value.getImageUrl();
    if (url == null || url.isBlank()) {
      return CompletableFuture.completedFuture(EnrichResult.permanentFailure(
          new DlqRecord(DlqStage.IMAGE_ENRICH.name(), toRawString(value),
              "IMAGE event has neither imageUrl nor imageObjectKey", Instant.now())));
    }

    String extension = ImageFormat.extensionForUrl(url);
    String objectKey = ImageObjectKey.of(value.getDate(), value.getId(), extension);
    String contentType = ImageFormat.contentTypeForExtension(extension);

    return CompletableFuture
        .supplyAsync(() -> statSize(bucket, objectKey), executor)
        .thenCompose(existingSize -> existingSize != null
            ? CompletableFuture.completedFuture(
                EnrichResult.success(withObjectKey(value, objectKey), existingSize))
            : fetch(url).thenComposeAsync(bytes -> {
                putObjectUnchecked(bucket, objectKey, bytes, contentType);
                return CompletableFuture.completedFuture(
                    EnrichResult.success(withObjectKey(value, objectKey), (long) bytes.length));
              }, executor))
        .handle((res, err) -> err == null ? res : classify(value, err));
  }

  /**
   * Single async HTTP GET with read timeout, status classification, an image Content-Type
   * check, and the 10 MB cap. sendAsync resolves its future when the response headers arrive
   * while the body is still streaming, so the blocking readNBytes runs via thenApplyAsync on
   * the bounded executor rather than on the HttpClient's own thread, keeping every external
   * I/O path bounded.
   */
  private CompletableFuture<byte[]> fetch(String url) {
    // Host already SSRF-checked at ingestion (IMAGE_URL_ALLOWED_HOSTS) and the events topic carries
    // only backend-produced URLs, so it is not re-checked here; followRedirects(NEVER) blocks 3xx pivots.
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
          // An allowlisted host can still return a non-image body (HTML error/login page,
          // PDF). Retrying cannot turn it into an image, and storing it under a fabricated
          // image content-type would be silently wrong, so an absent or non-image/* Content-Type
          // is a permanent failure.
          String contentType = resp.headers().firstValue("Content-Type").orElse("");
          if (!ImageRules.isImageContentType(contentType)) {
            throw new PermanentImageException(
                "imageUrl response is not an image (Content-Type="
                    + (contentType.isBlank() ? "<absent>" : contentType) + ")");
          }
          try (InputStream body = resp.body()) {
            byte[] bytes = body.readNBytes((int) (ImageRules.MAX_IMAGE_BYTES + 1));
            if (bytes.length > ImageRules.MAX_IMAGE_BYTES) {
              throw new PermanentImageException(
                  "Image response exceeds " + (ImageRules.MAX_IMAGE_BYTES / 1024 / 1024) + " MB cap");
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
    DlqRecord record = new DlqRecord(DlqStage.IMAGE_ENRICH.name(), toRawString(value), msg, Instant.now());
    if (cause instanceof PermanentImageException) {
      log.warn("Permanent image failure for id={}: {}", value.getId(), msg);
      return EnrichResult.permanentFailure(record);
    }
    log.warn("Retryable image failure for id={}: {}", value.getId(), msg);
    retryableFailures.inc();
    return EnrichResult.retryableFailure(record);
  }

  long retryableFailureCount() {
    return retryableFailures.getCount();
  }

  long uploadCount() {
    return uploadCount.getCount();
  }

  long uploadNanos() {
    return uploadNanos.getCount();
  }

  private static Throwable unwrap(Throwable t) {
    Throwable c = t;
    while ((c instanceof java.util.concurrent.CompletionException
        || c instanceof java.util.concurrent.ExecutionException) && c.getCause() != null) {
      c = c.getCause();
    }
    return c;
  }

  /** Object size, or null if absent (NoSuchKey). Other errors propagate so the URL path retries. */
  private Long statSize(String bucket, String objectKey) {
    try {
      return minio.statObject(StatObjectArgs.builder().bucket(bucket).object(objectKey).build()).size();
    } catch (ErrorResponseException e) {
      if ("NoSuchKey".equals(e.errorResponse().code())) {
        return null;
      }
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Like statSize but swallows every failure (returns null), so a passthrough never fails on a stat error. */
  private Long statSizeBestEffort(String bucket, String objectKey) {
    try {
      return statSize(bucket, objectKey);
    } catch (RuntimeException e) {
      log.warn("Size stat failed for passthrough object {}; omitting from size histogram: {}",
          objectKey, e.getMessage());
      return null;
    }
  }

  /**
   * Uploads the fetched bytes to MinIO and meters the call (count and duration) on success;
   * wraps checked exceptions so they classify as retryable. A failed putObject is not metered,
   * so the upload metrics reflect only genuine writes.
   */
  private void putObjectUnchecked(String bucket, String objectKey, byte[] bytes, String contentType) {
    try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
      long start = System.nanoTime();
      minio.putObject(PutObjectArgs.builder()
          .bucket(bucket).object(objectKey)
          .stream(in, bytes.length, -1)
          .contentType(contentType)
          .build());
      uploadNanos.inc(System.nanoTime() - start);
      uploadCount.inc();
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

  @Override
  public void close() {
    if (ownedExecutor != null) {
      ownedExecutor.shutdownNow();
    }
  }
}
