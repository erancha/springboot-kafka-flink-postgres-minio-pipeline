package com.webcharm.backend.minio;

import com.webcharm.backend.storage.ObjectStoreException;
import io.minio.MinioClient;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MinioUploadServiceTest {

  @Mock MinioClient minioClient;
  MinioUploadService service;

  @BeforeEach
  void setUp() {
    service = new MinioUploadService(minioClient, "images");
  }

  @Test
  void upload_jpegFile_returnsKeyWithCorrectDateAndExtension() throws Exception {
    when(minioClient.putObject(any())).thenReturn(null);
    UUID id = UUID.fromString("00000000-0000-0000-0000-000000000001");
    Instant eventTime = Instant.parse("2026-05-16T10:00:00Z");
    MockMultipartFile file = new MockMultipartFile(
        "file", "photo.jpg", "image/jpeg", new byte[]{1, 2, 3});

    String key = service.upload(id, eventTime, file);

    assertEquals("images/2026-05-16/00000000-0000-0000-0000-000000000001.jpg", key);
    verify(minioClient).putObject(any());
  }

  @Test
  void upload_pngFile_returnsKeyWithPngExtension() throws Exception {
    when(minioClient.putObject(any())).thenReturn(null);
    UUID id = UUID.fromString("00000000-0000-0000-0000-000000000002");
    MockMultipartFile file = new MockMultipartFile(
        "file", "photo.png", "image/png", new byte[]{1, 2, 3});

    String key = service.upload(id, Instant.parse("2026-05-16T10:00:00Z"), file);

    assertTrue(key.endsWith(".png"), "Expected .png extension, got: " + key);
  }

  @Test
  void upload_streamsInputWithoutBuffering() throws Exception {
    // getBytes() throws — proves the implementation does not buffer the file in heap.
    MultipartFile file = mock(MultipartFile.class);
    when(file.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[]{1, 2, 3}));
    when(file.getSize()).thenReturn(3L);
    when(file.getContentType()).thenReturn("image/jpeg");
    when(minioClient.putObject(any())).thenReturn(null);

    service.upload(UUID.randomUUID(), Instant.now(), file);

    verify(file, never()).getBytes();
  }

  @Test
  void upload_minioThrows_wrapsInObjectStoreException() throws Exception {
    doThrow(new java.io.IOException("connection refused"))
        .when(minioClient).putObject(any());
    MockMultipartFile file = new MockMultipartFile(
        "file", "photo.jpg", "image/jpeg", new byte[]{1, 2, 3});

    assertThrows(ObjectStoreException.class, () ->
        service.upload(UUID.randomUUID(), Instant.now(), file));
  }
}
