package com.webcharm.pipeline.functions;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.webcharm.pipeline.types.DlqStage;
import com.webcharm.pipeline.types.EnrichResult;
import com.webcharm.pipeline.types.ProcessedEvent;
import io.minio.MinioClient;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.ErrorResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Verifies the async enrichment outcomes: passthrough, statObject idempotency guard,
 * size cap, non-image/absent Content-Type guard, SSRF 3xx guard, 4xx permanent,
 * 5xx/IOException retryable, success, and the retryable-failure and successful-upload
 * Prometheus metrics. Uses a direct (same-thread)
 * executor and mocked clients so CompletableFutures resolve synchronously and assertions
 * are deterministic without Docker.
 */
@ExtendWith(MockitoExtension.class)
class MinioAsyncImageFunctionTest {

  @Mock MinioClient minioClient;
  @Mock HttpClient httpClient;

  private MinioAsyncImageFunction newFn() {
    return new MinioAsyncImageFunction(minioClient, httpClient, Runnable::run);
  }

  private static ProcessedEvent urlEvent(String url) {
    return new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "test",
        null, url, null, LocalDate.now());
  }

  private static HttpResponse<InputStream> resp(int status, byte[] body) {
    return resp(status, body, "image/jpeg");
  }

  /**
   * Builds a mock HTTP response. For 2xx responses the Content-Type header is stubbed
   * (a null contentType yields a header-less response, modelling an origin that omits it);
   * the header is only stubbed on 2xx because the content-type guard is unreachable for
   * non-2xx, and stubbing it there would trip Mockito strict-stub checks.
   */
  @SuppressWarnings("unchecked")
  private static HttpResponse<InputStream> resp(int status, byte[] body, String contentType) {
    HttpResponse<InputStream> r = mock(HttpResponse.class);
    when(r.statusCode()).thenReturn(status);
    if (status / 100 == 2) {
      Map<String, List<String>> headers = contentType == null
          ? Map.of()
          : Map.of("Content-Type", List.of(contentType));
      lenient().when(r.headers()).thenReturn(HttpHeaders.of(headers, (a, b) -> true));
    }
    if (body != null) {
      when(r.body()).thenReturn(new ByteArrayInputStream(body));
    }
    return r;
  }

  private static ErrorResponseException noSuchKey() {
    ErrorResponse er = mock(ErrorResponse.class);
    when(er.code()).thenReturn("NoSuchKey");
    return new ErrorResponseException(er, null, "");
  }

  /**
   * An event whose imageObjectKey is already set (the backend uploaded the bytes) has nothing
   * to enrich, so it passes straight through as success and no HTTP fetch is attempted.
   */
  @Test
  void imageObjectKeyAlreadySet_passesThroughAsSuccess() throws Exception {
    ProcessedEvent e = new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, "images/2026-05-17/pre.jpg", LocalDate.now());

    EnrichResult r = newFn().enrich(e).join();

    assertTrue(r.isSuccess());
    assertEquals("images/2026-05-17/pre.jpg", r.success().getImageObjectKey());
    verify(httpClient, never()).sendAsync(any(), any());
  }

  /**
   * When MinIO already holds the target object (statObject succeeds), enrichment is idempotent:
   * it skips the fetch and upload and returns success, so a checkpoint replay cannot duplicate work.
   */
  @Test
  void objectAlreadyExists_skipsFetchAndUpload() throws Exception {
    when(minioClient.statObject(any())).thenReturn(mock(StatObjectResponse.class));

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/photo.jpg")).join();

    assertTrue(r.isSuccess());
    assertNull(r.success().getImageUrl());
    assertTrue(r.success().getImageObjectKey().endsWith(".jpg"));
    verify(httpClient, never()).sendAsync(any(), any());
    verify(minioClient, never()).putObject(any());
  }

  /**
   * The object is absent (statObject reports NoSuchKey) and the URL returns 200, so the image
   * is fetched and uploaded to MinIO. Expected success with a putObject call as the happy path.
   */
  @Test
  void objectAbsent_fetchesAndUploads_success() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(200, new byte[] {1, 2, 3})))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/photo.jpg")).join();

    assertTrue(r.isSuccess());
    verify(minioClient).putObject(any());
  }

  /**
   * A 4xx is a client error that retrying cannot fix, so it is classified permanent and routed
   * to the DLQ stamped with stage IMAGE_ENRICH. Expected not success and not retryable.
   */
  @Test
  void response4xx_permanentFailure() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(404, null)))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/missing.jpg")).join();

    assertFalse(r.isSuccess());
    assertFalse(r.isRetryable());
    assertNotNull(r.failure());
    assertEquals(DlqStage.IMAGE_ENRICH, r.failure().stage());
  }

  /**
   * The HTTP client follows no redirects as an SSRF guard, so a 3xx is handled like any other
   * non-2xx: permanent and never retried, so an allowlisted host cannot redirect the socket to
   * an internal endpoint. Expected not success and not retryable.
   */
  @Test
  void response3xxRedirect_permanentFailure_ssrfGuard() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(302, null)))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/a.jpg")).join();

    assertFalse(r.isSuccess());
    assertFalse(r.isRetryable());
  }

  /**
   * A 5xx is a transient server-side error that may succeed later, so it is classified retryable
   * and the framework async retry re-attempts it before it can reach the DLQ. Expected retryable.
   */
  @Test
  void response5xx_retryableFailure() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(503, null)))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/a.jpg")).join();

    assertFalse(r.isSuccess());
    assertTrue(r.isRetryable());
  }

  /**
   * A transport failure (connection reset) during the fetch is transient and may succeed on a
   * retry, so it is classified retryable rather than permanent. Expected retryable.
   */
  @Test
  void ioExceptionFromFetch_retryableFailure() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.failedFuture(new IOException("connection reset")))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/a.jpg")).join();

    assertFalse(r.isSuccess());
    assertTrue(r.isRetryable());
  }

  /**
   * A body exceeding the 10 MB cap will never fit no matter how many times it is retried, so it
   * is classified permanent and fails fast. Expected not success and not retryable.
   */
  @Test
  void responseOversized_permanentFailure() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(200, new byte[10 * 1024 * 1024 + 1])))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/huge.jpg")).join();

    assertFalse(r.isSuccess());
    assertFalse(r.isRetryable());
  }

  /**
   * A 200 whose Content-Type is not image/* (e.g. an HTML error page returned by an allowlisted
   * host) is not an image and retrying cannot make it one, so it is classified permanent and
   * never uploaded to MinIO. Expected not success, not retryable, and no putObject.
   */
  @Test
  void response200_nonImageContentType_permanentFailure() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(200, null, "text/html; charset=utf-8")))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/login.html")).join();

    assertFalse(r.isSuccess());
    assertFalse(r.isRetryable());
    assertEquals(DlqStage.IMAGE_ENRICH, r.failure().stage());
    verify(minioClient, never()).putObject(any());
  }

  /**
   * A 200 with no Content-Type header is unverifiable as an image; storing it under a fabricated
   * image content-type is exactly the misleading behavior the guard prevents, so an absent
   * Content-Type is permanent. Expected not success, not retryable, and no putObject.
   */
  @Test
  void response200_missingContentType_permanentFailure() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(200, null, null)))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/a.jpg")).join();

    assertFalse(r.isSuccess());
    assertFalse(r.isRetryable());
    verify(minioClient, never()).putObject(any());
  }

  /**
   * An IMAGE event carrying neither an imageUrl nor an imageObjectKey can never be enriched, so
   * it fails permanently without any fetch. Expected not success and not retryable.
   */
  @Test
  void noUrlNoKey_permanentFailure() throws Exception {
    ProcessedEvent e = new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, null, LocalDate.now());

    EnrichResult r = newFn().enrich(e).join();

    assertFalse(r.isSuccess());
    assertFalse(r.isRetryable());
  }

  /**
   * The blocking response-body read must run on the function's bounded executor, not on the
   * HttpClient's internal completion thread, so a slow body cannot starve the HttpClient's pool.
   * Expected success with the body observed on an owned-exec thread; fails if it ran on the
   * foreign completion thread.
   */
  @Test
  @SuppressWarnings("unchecked")
  void bodyRead_runsOnInjectedExecutor_notHttpClientCompletionThread() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);

    ExecutorService owned = Executors.newFixedThreadPool(
        2, r -> new Thread(r, "owned-exec-" + UUID.randomUUID()));

    HttpResponse<InputStream> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    lenient().when(response.headers()).thenReturn(
        HttpHeaders.of(Map.of("Content-Type", List.of("image/jpeg")), (a, b) -> true));
    AtomicReference<String> bodyReadThread = new AtomicReference<>();
    when(response.body()).thenAnswer(inv -> {
      bodyReadThread.set(Thread.currentThread().getName());
      return new ByteArrayInputStream(new byte[] {1, 2, 3});
    });

    // sendAsync resolves later on a foreign thread, mirroring the HttpClient's own
    // internal executor completing the response while the body is still streaming.
    CompletableFuture<HttpResponse<InputStream>> pending = new CompletableFuture<>();
    doAnswer(inv -> {
      Thread completer = new Thread(() -> {
        try {
          Thread.sleep(150);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        pending.complete(response);
      }, "httpclient-internal");
      completer.setDaemon(true);
      completer.start();
      return pending;
    }).when(httpClient).sendAsync(any(), any());

    EnrichResult r = new MinioAsyncImageFunction(minioClient, httpClient, owned)
        .enrich(urlEvent("https://cdn.example.com/p.jpg"))
        .get(5, TimeUnit.SECONDS);
    owned.shutdownNow();

    assertTrue(r.isSuccess());
    assertNotNull(bodyReadThread.get(), "body was never read");
    assertTrue(bodyReadThread.get().startsWith("owned-exec-"),
        "blocking body read must run on the injected executor, but ran on: "
            + bodyReadThread.get());
  }

  /**
   * A retryable failure (5xx) increments the Prometheus retryable-failure counter exactly once,
   * so retry pressure on the IMAGE branch is observable. Expected retryable and counter is 1.
   */
  @Test
  void retryableFailure_incrementsRetryableCounter() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(503, null)))
        .when(httpClient).sendAsync(any(), any());

    MinioAsyncImageFunction fn = newFn();
    EnrichResult r = fn.enrich(urlEvent("https://cdn.example.com/a.jpg")).join();

    assertTrue(r.isRetryable());
    assertEquals(1, fn.retryableFailureCount());
  }

  /**
   * A permanent failure (4xx) is not a retry candidate, so the retryable-failure counter is not
   * incremented and stays at 0. Guards against permanent failures inflating retry metrics.
   */
  @Test
  void permanentFailure_doesNotIncrementRetryableCounter() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(404, null)))
        .when(httpClient).sendAsync(any(), any());

    MinioAsyncImageFunction fn = newFn();
    EnrichResult r = fn.enrich(urlEvent("https://cdn.example.com/missing.jpg")).join();

    assertFalse(r.isRetryable());
    assertEquals(0, fn.retryableFailureCount());
  }

  /**
   * A successful enrichment never touches the retryable-failure counter, so the metric reflects
   * only genuine retry pressure. Expected counter is 0.
   */
  @Test
  void success_doesNotIncrementRetryableCounter() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(200, new byte[] {1, 2, 3})))
        .when(httpClient).sendAsync(any(), any());

    MinioAsyncImageFunction fn = newFn();
    fn.enrich(urlEvent("https://cdn.example.com/photo.jpg")).join();

    assertEquals(0, fn.retryableFailureCount());
  }

  /**
   * A successful fetch-and-upload meters exactly one MinIO upload and records a positive
   * putObject duration, so DATA-vs-IMAGE throughput and average upload latency are observable
   * in Prometheus. Expected success, upload count 1, and upload nanos greater than 0.
   */
  @Test
  void successfulUpload_isMeteredOnceWithPositiveDuration() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(200, new byte[] {1, 2, 3})))
        .when(httpClient).sendAsync(any(), any());

    MinioAsyncImageFunction fn = newFn();
    EnrichResult r = fn.enrich(urlEvent("https://cdn.example.com/photo.jpg")).join();

    assertTrue(r.isSuccess());
    verify(minioClient).putObject(any());
    assertEquals(1, fn.uploadCount());
    assertTrue(fn.uploadNanos() > 0, "a successful upload must record a positive duration");
  }

  /**
   * An idempotency hit (the object already exists) skips putObject entirely, so it is not
   * counted as an upload and cannot inflate throughput or skew average upload latency.
   * Expected success with upload count 0.
   */
  @Test
  void idempotencyHit_isNotMeteredAsUpload() throws Exception {
    when(minioClient.statObject(any())).thenReturn(mock(StatObjectResponse.class));

    MinioAsyncImageFunction fn = newFn();
    EnrichResult r = fn.enrich(urlEvent("https://cdn.example.com/photo.jpg")).join();

    assertTrue(r.isSuccess());
    verify(minioClient, never()).putObject(any());
    assertEquals(0, fn.uploadCount());
    assertEquals(0, fn.uploadNanos());
  }

  /**
   * A permanent failure (4xx) never reaches putObject, so it is not counted as an upload and
   * the upload metrics reflect only genuine writes. Expected not success with upload count 0.
   */
  @Test
  void permanentFailure_isNotMeteredAsUpload() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(404, null)))
        .when(httpClient).sendAsync(any(), any());

    MinioAsyncImageFunction fn = newFn();
    EnrichResult r = fn.enrich(urlEvent("https://cdn.example.com/missing.jpg")).join();

    assertFalse(r.isSuccess());
    assertEquals(0, fn.uploadCount());
  }

  /**
   * The freshly fetched body length is the captured image size, so the size-bucket histogram
   * reflects exactly what was uploaded on the URL path.
   */
  @Test
  void freshUpload_capturesFetchedByteSize() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(200, new byte[] {1, 2, 3, 4, 5})))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/photo.jpg")).join();

    assertTrue(r.isSuccess());
    assertEquals(5L, r.imageBytes());
  }

  /**
   * On an idempotency hit the fetch is skipped, so the captured size comes from the existing
   * object's stat rather than a re-download.
   */
  @Test
  void idempotencyHit_capturesStoredObjectSize() throws Exception {
    StatObjectResponse stat = mock(StatObjectResponse.class);
    when(stat.size()).thenReturn(2048L);
    when(minioClient.statObject(any())).thenReturn(stat);

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/photo.jpg")).join();

    assertTrue(r.isSuccess());
    assertEquals(2048L, r.imageBytes());
    verify(httpClient, never()).sendAsync(any(), any());
  }

  /**
   * A passthrough event (backend already uploaded the bytes) has no body for Flink to measure, so
   * the size is read from the stored object's stat. Expected success carrying that stat size.
   */
  @Test
  void passthrough_capturesStoredObjectSizeViaStat() throws Exception {
    StatObjectResponse stat = mock(StatObjectResponse.class);
    when(stat.size()).thenReturn(7L * 1024 * 1024);
    when(minioClient.statObject(any())).thenReturn(stat);
    ProcessedEvent e = new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, "images/2026-05-17/pre.jpg", LocalDate.now());

    EnrichResult r = newFn().enrich(e).join();

    assertTrue(r.isSuccess());
    assertEquals(7L * 1024 * 1024, r.imageBytes());
    verify(httpClient, never()).sendAsync(any(), any());
  }

  /**
   * A stat failure on the passthrough branch is best-effort: it leaves the size unknown (null) and
   * still yields success, so a MinIO blip drops the event from the histogram without failing it.
   */
  @Test
  void passthrough_statFailure_succeedsWithUnknownSize() throws Exception {
    when(minioClient.statObject(any())).thenThrow(new RuntimeException("minio down"));
    ProcessedEvent e = new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, "images/2026-05-17/pre.jpg", LocalDate.now());

    EnrichResult r = newFn().enrich(e).join();

    assertTrue(r.isSuccess());
    assertNull(r.imageBytes());
  }

  /**
   * The object key extension is derived from the URL path, so a .png URL produces a key ending
   * in .png (extension and content-type routing). Expected success with a .png key suffix.
   */
  @Test
  void pngUrl_keySuffixedPng() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);
    doReturn(CompletableFuture.completedFuture(resp(200, new byte[] {1})))
        .when(httpClient).sendAsync(any(), any());

    EnrichResult r = newFn().enrich(urlEvent("https://cdn.example.com/banner.png")).join();

    assertTrue(r.isSuccess());
    assertTrue(r.success().getImageObjectKey().endsWith(".png"));
  }
}
