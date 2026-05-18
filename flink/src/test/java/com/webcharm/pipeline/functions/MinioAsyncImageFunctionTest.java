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
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDate;
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
 * size cap, SSRF 3xx guard, 4xx permanent, 5xx/IOException retryable, success.
 * Uses a direct (same-thread) executor and mocked clients so CompletableFutures resolve
 * synchronously and assertions are deterministic without Docker.
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

  @SuppressWarnings("unchecked")
  private static HttpResponse<InputStream> resp(int status, byte[] body) {
    HttpResponse<InputStream> r = mock(HttpResponse.class);
    when(r.statusCode()).thenReturn(status);
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

  @Test
  void imageObjectKeyAlreadySet_passesThroughAsSuccess() throws Exception {
    ProcessedEvent e = new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, "images/2026-05-17/pre.jpg", LocalDate.now());

    EnrichResult r = newFn().enrich(e).join();

    assertTrue(r.isSuccess());
    assertEquals("images/2026-05-17/pre.jpg", r.success().getImageObjectKey());
    verify(httpClient, never()).sendAsync(any(), any());
  }

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

  @Test
  void noUrlNoKey_permanentFailure() throws Exception {
    ProcessedEvent e = new ProcessedEvent(UUID.randomUUID(), "IMAGE", Instant.now(), "t",
        null, null, null, LocalDate.now());

    EnrichResult r = newFn().enrich(e).join();

    assertFalse(r.isSuccess());
    assertFalse(r.isRetryable());
  }

  @Test
  void bodyRead_runsOnInjectedExecutor_notHttpClientCompletionThread() throws Exception {
    ErrorResponseException nsk = noSuchKey();
    when(minioClient.statObject(any())).thenThrow(nsk);

    ExecutorService owned = Executors.newFixedThreadPool(
        2, r -> new Thread(r, "owned-exec-" + UUID.randomUUID()));

    HttpResponse<InputStream> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
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
