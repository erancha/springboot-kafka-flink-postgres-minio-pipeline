package com.webcharm.pipeline.functions;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.webcharm.pipeline.types.DlqRecord;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Verifies validateImageUrl (SSRF guard), statObject existence guard, DLQ side-output routing,
 * HTTP response-size cap, and fetch retry logic.
 */
@ExtendWith(MockitoExtension.class)
class MinioUploadFunctionTest {

  @Mock
  MinioClient minioClient;
  @Mock
  HttpClient httpClient;

  // ── DLQ routing ───────────────────────────────────────────────────────────

  /**
   * Subclass that captures processElement side outputs and main output without a Flink runtime,
   * following the same pattern used by ParseEventFunctionTest.TestableFn.
   */
  static class TestableFn extends MinioUploadFunction {
    final List<DlqRecord> dlqCapture = new ArrayList<>();
    final List<ProcessedEvent> mainCapture = new ArrayList<>();

    TestableFn(MinioClient minio, HttpClient http) {
      super(minio, http);
    }

    void run(ProcessedEvent event) throws Exception {
      Context ctx = new Context() {
        @Override
        public Long timestamp() {
          return null;
        }

        @Override
        public TimerService timerService() {
          return null;
        }

        @Override
        public <X> void output(OutputTag<X> tag, X val) {
          if (val instanceof DlqRecord r)
            dlqCapture.add(r);
        }
      };
      Collector<ProcessedEvent> col = new Collector<>() {
        public void collect(ProcessedEvent e) {
          mainCapture.add(e);
        }

        public void close() {
        }
      };
      processElement(event, ctx, col);
    }
  }

  // ── validateImageUrl ──────────────────────────────────────────────────────

  @Test
  void validateImageUrl_validHttpUrl_doesNotThrow() {
    assertDoesNotThrow(() -> MinioUploadFunction.validateImageUrl("http://example.com/img.jpg"));
  }

  @Test
  void validateImageUrl_validHttpsUrl_doesNotThrow() {
    assertDoesNotThrow(() -> MinioUploadFunction.validateImageUrl("https://example.com/img.jpg"));
  }

  @Test
  void validateImageUrl_fileScheme_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class,
        () -> MinioUploadFunction.validateImageUrl("file:///etc/passwd"));
  }

  @Test
  void validateImageUrl_ftpScheme_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class,
        () -> MinioUploadFunction.validateImageUrl("ftp://example.com/img.jpg"));
  }

  @Test
  void validateImageUrl_noHost_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class,
        () -> MinioUploadFunction.validateImageUrl("https:///img.jpg"));
  }

  @Test
  void validateImageUrl_malformedUri_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class,
        () -> MinioUploadFunction.validateImageUrl("not a url at all"));
  }

  // ── statObject existence guard ────────────────────────────────────────────

  @Test
  void map_urlEvent_objectAlreadyExists_skipsHttpFetchAndUpload() throws Exception {
    when(minioClient.statObject(any())).thenReturn(mock(StatObjectResponse.class));

    ProcessedEvent event = urlEvent("https://cdn.example.com/photo.jpg");
    ProcessedEvent result = new MinioUploadFunction(minioClient, httpClient).upload(event);

    verify(httpClient, never()).send(any(), any());
    verify(minioClient, never()).putObject(any());
    assertNotNull(result.getImageObjectKey());
    assertTrue(result.getImageObjectKey().endsWith(".jpg")); // photo.jpg → .jpg
    assertNull(result.getImageUrl());
  }

  @Test
  @SuppressWarnings("unchecked")
  void map_urlEvent_objectAbsent_fetchesAndUploads() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    HttpResponse<InputStream> httpResp = mock(HttpResponse.class);
    when(httpResp.statusCode()).thenReturn(200);
    when(httpResp.body()).thenReturn(new ByteArrayInputStream(new byte[] { 1, 2, 3 }));
    doReturn(httpResp).when(httpClient).send(any(), any());

    ProcessedEvent event = urlEvent("https://cdn.example.com/photo.jpg");
    new MinioUploadFunction(minioClient, httpClient).upload(event);

    verify(httpClient).send(any(), any());
    verify(minioClient).putObject(any());
  }

  // ── HTTP size cap ─────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void upload_urlEvent_responseExceedsSizeCap_throwsIllegalState() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    byte[] oversized = new byte[10 * 1024 * 1024 + 1];
    HttpResponse<InputStream> httpResp = mock(HttpResponse.class);
    when(httpResp.statusCode()).thenReturn(200);
    when(httpResp.body()).thenReturn(new ByteArrayInputStream(oversized));
    doReturn(httpResp).when(httpClient).send(any(), any());

    assertThrows(IllegalStateException.class,
        () -> new MinioUploadFunction(minioClient, httpClient)
            .upload(urlEvent("https://cdn.example.com/huge.jpg")));
  }

  // ── retry logic ───────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void upload_urlEvent_transientErrorThenSuccess_retriesAndUploads() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    HttpResponse<InputStream> httpResp = mock(HttpResponse.class);
    when(httpResp.statusCode()).thenReturn(200);
    when(httpResp.body()).thenReturn(new ByteArrayInputStream(new byte[] { 1, 2, 3 }));
    doThrow(new IOException("connection reset"))
        .doReturn(httpResp)
        .when(httpClient).send(any(), any());

    new MinioUploadFunction(minioClient, httpClient)
        .upload(urlEvent("https://cdn.example.com/photo.jpg"));

    verify(httpClient, times(2)).send(any(), any());
    verify(minioClient).putObject(any());
  }

  @Test
  void upload_urlEvent_allFetchAttemptsExhausted_throwsIllegalState() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    doThrow(new IOException("timeout")).when(httpClient).send(any(), any());

    assertThrows(IllegalStateException.class,
        () -> new MinioUploadFunction(minioClient, httpClient)
            .upload(urlEvent("https://cdn.example.com/photo.jpg")));

    verify(httpClient, times(3)).send(any(), any());
  }

  // ── passthrough ───────────────────────────────────────────────────────────

  @Test
  void upload_imageObjectKeyAlreadySet_returnsWithoutFetching() throws Exception {
    ProcessedEvent event = new ProcessedEvent(
        UUID.randomUUID(), "IMAGE", Instant.now(), "test",
        null, null, "images/2026-05-16/pre-set-key.jpg", LocalDate.now());

    ProcessedEvent result = new MinioUploadFunction(minioClient, httpClient).upload(event);

    assertEquals("images/2026-05-16/pre-set-key.jpg", result.getImageObjectKey());
    verify(httpClient, never()).send(any(), any());
    verify(minioClient, never()).putObject(any());
  }

  // ── IMAGE_URL_ALLOWED_HOSTS allowlist ────────────────────────────────────

  @Test
  void validateImageUrl_hostInAllowlist_doesNotThrow() {
    assertDoesNotThrow(() ->
        MinioUploadFunction.validateImageUrl("https://cdn.example.com/img.jpg", "example.com,cdn.example.com"));
  }

  @Test
  void validateImageUrl_hostNotInAllowlist_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () ->
        MinioUploadFunction.validateImageUrl("https://blocked.com/img.jpg", "example.com"));
  }

  // ── 5xx response triggers retry ───────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void upload_urlEvent_server5xxThenSuccess_retriesAndUploads() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    HttpResponse<InputStream> serverError = mock(HttpResponse.class);
    when(serverError.statusCode()).thenReturn(503);

    HttpResponse<InputStream> success = mock(HttpResponse.class);
    when(success.statusCode()).thenReturn(200);
    when(success.body()).thenReturn(new ByteArrayInputStream(new byte[]{1, 2, 3}));

    doReturn(serverError).doReturn(success).when(httpClient).send(any(), any());

    new MinioUploadFunction(minioClient, httpClient)
        .upload(urlEvent("https://cdn.example.com/photo.jpg"));

    verify(httpClient, times(2)).send(any(), any());
    verify(minioClient).putObject(any());
  }

  // ── DLQ routing via processElement ───────────────────────────────────────

  @Test
  void processElement_invalidSchemeUrl_routesToDlq() throws Exception {
    TestableFn fn = new TestableFn(minioClient, httpClient);
    fn.run(urlEvent("file:///etc/passwd"));

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void processElement_urlEvent_404Response_routesToDlq() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    HttpResponse<InputStream> notFoundResp = mock(HttpResponse.class);
    when(notFoundResp.statusCode()).thenReturn(404);
    doReturn(notFoundResp).when(httpClient).send(any(), any());

    TestableFn fn = new TestableFn(minioClient, httpClient);
    fn.run(urlEvent("https://cdn.example.com/photo.jpg"));

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void processElement_urlEvent_oversizedResponse_routesToDlq() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    HttpResponse<InputStream> httpResp = mock(HttpResponse.class);
    when(httpResp.statusCode()).thenReturn(200);
    when(httpResp.body()).thenReturn(new ByteArrayInputStream(new byte[10 * 1024 * 1024 + 1]));
    doReturn(httpResp).when(httpClient).send(any(), any());

    TestableFn fn = new TestableFn(minioClient, httpClient);
    fn.run(urlEvent("https://cdn.example.com/huge.jpg"));

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
  }

  // ── extension detection ───────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void upload_pngUrl_keySuffixedWithPng() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    HttpResponse<InputStream> httpResp = mock(HttpResponse.class);
    when(httpResp.statusCode()).thenReturn(200);
    when(httpResp.body()).thenReturn(new ByteArrayInputStream(new byte[]{1, 2, 3}));
    doReturn(httpResp).when(httpClient).send(any(), any());

    ProcessedEvent result = new MinioUploadFunction(minioClient, httpClient)
        .upload(urlEvent("https://cdn.example.com/banner.png"));

    assertTrue(result.getImageObjectKey().endsWith(".png"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void upload_webpUrl_keySuffixedWithWebp() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    HttpResponse<InputStream> httpResp = mock(HttpResponse.class);
    when(httpResp.statusCode()).thenReturn(200);
    when(httpResp.body()).thenReturn(new ByteArrayInputStream(new byte[]{1, 2, 3}));
    doReturn(httpResp).when(httpClient).send(any(), any());

    ProcessedEvent result = new MinioUploadFunction(minioClient, httpClient)
        .upload(urlEvent("https://cdn.example.com/photo.webp"));

    assertTrue(result.getImageObjectKey().endsWith(".webp"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void upload_urlWithNoRecognisedExtension_defaultsToJpg() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    HttpResponse<InputStream> httpResp = mock(HttpResponse.class);
    when(httpResp.statusCode()).thenReturn(200);
    when(httpResp.body()).thenReturn(new ByteArrayInputStream(new byte[]{1, 2, 3}));
    doReturn(httpResp).when(httpClient).send(any(), any());

    ProcessedEvent result = new MinioUploadFunction(minioClient, httpClient)
        .upload(urlEvent("https://picsum.photos/200")); // no extension in path

    assertTrue(result.getImageObjectKey().endsWith(".jpg"));
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private static ProcessedEvent urlEvent(String url) {
    return new ProcessedEvent(
        UUID.randomUUID(), "IMAGE", Instant.now(), "test",
        null, url, null, LocalDate.now());
  }
}
