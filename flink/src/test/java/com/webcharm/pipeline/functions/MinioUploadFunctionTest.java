package com.webcharm.pipeline.functions;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.webcharm.pipeline.types.ProcessedEvent;
import io.minio.MinioClient;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.ErrorResponse;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Verifies validateImageUrl (SSRF guard) and the statObject existence guard introduced for
 * replay-safe MinIO uploads.
 */
@ExtendWith(MockitoExtension.class)
class MinioUploadFunctionTest {

  @Mock MinioClient minioClient;
  @Mock HttpClient  httpClient;

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
    ProcessedEvent result = new MinioUploadFunction(minioClient, httpClient).map(event);

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
    new MinioUploadFunction(minioClient, httpClient).map(event);

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
