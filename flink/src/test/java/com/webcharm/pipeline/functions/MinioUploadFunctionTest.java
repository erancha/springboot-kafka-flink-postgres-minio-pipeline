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
 * Verifies validateImageUrl (SSRF guard), statObject existence guard, and DLQ side-output routing.
 */
@ExtendWith(MockitoExtension.class)
class MinioUploadFunctionTest {

  @Mock MinioClient minioClient;
  @Mock HttpClient  httpClient;

  // ── DLQ routing ───────────────────────────────────────────────────────────

  /**
   * Subclass that captures processElement side outputs and main output without a Flink runtime,
   * following the same pattern used by ParseEventFunctionTest.TestableFn.
   */
  static class TestableFn extends MinioUploadFunction {
    final List<DlqRecord>      dlqCapture  = new ArrayList<>();
    final List<ProcessedEvent> mainCapture = new ArrayList<>();

    TestableFn(MinioClient minio, HttpClient http) { super(minio, http); }

    void run(ProcessedEvent event) throws Exception {
      Context ctx = new Context() {
        @Override public Long timestamp() { return null; }
        @Override public TimerService timerService() { return null; }
        @Override public <X> void output(OutputTag<X> tag, X val) {
          if (val instanceof DlqRecord r) dlqCapture.add(r);
        }
      };
      Collector<ProcessedEvent> col = new Collector<>() {
        public void collect(ProcessedEvent e) { mainCapture.add(e); }
        public void close() {}
      };
      processElement(event, ctx, col);
    }
  }

  /** An invalid Base64 payload must route to UPLOAD_ERROR_TAG and not reach the main output. */
  @Test
  void processElement_invalidBase64_routesToDlq() throws Exception {
    TestableFn fn = new TestableFn(minioClient, httpClient);
    ProcessedEvent bad = new ProcessedEvent(
        UUID.randomUUID(), "IMAGE", Instant.now(), "test",
        null, null, "!!!not-valid-base64!!!", "image/jpeg", null, LocalDate.now());

    fn.run(bad);

    assertEquals(1, fn.dlqCapture.size());
    assertTrue(fn.mainCapture.isEmpty());
    assertNotNull(fn.dlqCapture.get(0).error());
    assertNotNull(fn.dlqCapture.get(0).failedAt());
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

    ProcessedEvent event = urlEvent("https://cdn.example.com/photo.jpg", "image/png");
    ProcessedEvent result = new MinioUploadFunction(minioClient, httpClient).upload(event);

    verify(httpClient, never()).send(any(), any());
    verify(minioClient, never()).putObject(any());
    assertNotNull(result.getImageObjectKey());
    assertTrue(result.getImageObjectKey().endsWith(".png"));
    assertNull(result.getImageUrl());
  }

  @Test
  @SuppressWarnings("unchecked")
  void map_urlEvent_objectAbsent_fetchesAndUploads() throws Exception {
    ErrorResponse errResp = mock(ErrorResponse.class);
    when(errResp.code()).thenReturn("NoSuchKey");
    ErrorResponseException notFound = new ErrorResponseException(errResp, null, "");
    when(minioClient.statObject(any())).thenThrow(notFound);

    HttpResponse<byte[]> httpResp = mock(HttpResponse.class);
    when(httpResp.statusCode()).thenReturn(200);
    when(httpResp.body()).thenReturn(new byte[]{1, 2, 3});
    doReturn(httpResp).when(httpClient).send(any(), any());

    ProcessedEvent event = urlEvent("https://cdn.example.com/photo.jpg", "image/jpeg");
    new MinioUploadFunction(minioClient, httpClient).upload(event);

    verify(httpClient).send(any(), any());
    verify(minioClient).putObject(any());
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private static ProcessedEvent urlEvent(String url, String contentType) {
    return new ProcessedEvent(
        UUID.randomUUID(), "IMAGE", Instant.now(), "test", null,
        url, null, contentType, null, LocalDate.now());
  }
}
