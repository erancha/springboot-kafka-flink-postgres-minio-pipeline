package com.webcharm.backend.minio;

import com.webcharm.backend.storage.ImageUploadService;
import com.webcharm.backend.storage.ObjectStoreException;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

/** Uploads a multipart image to MinIO and returns the resulting object key. */
@Service
public class MinioUploadService implements ImageUploadService {

  private final MinioClient minioClient;
  private final String bucket;

  public MinioUploadService(MinioClient minioClient, @Value("${minio.bucket}") String bucket) {
    this.minioClient = minioClient;
    this.bucket = bucket;
  }

  /**
   * Uploads file bytes to MinIO under key images/{date}/{id}.{ext} and returns that key.
   * Throws ObjectStoreException if the MinIO call fails.
   * Lets IOException from file.getBytes() propagate (mapped to 500 by GlobalExceptionHandler).
   */
  public String upload(UUID id, Instant eventTime, MultipartFile file) throws IOException {
    String contentType = file.getContentType() != null ? file.getContentType() : "image/jpeg";
    String date = DateTimeFormatter.ISO_LOCAL_DATE.format(eventTime.atZone(ZoneOffset.UTC));
    String objectKey = "images/" + date + "/" + id + guessExtension(contentType);

    byte[] bytes = file.getBytes();
    try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
      minioClient.putObject(
          PutObjectArgs.builder()
              .bucket(bucket).object(objectKey)
              .stream(in, bytes.length, -1)
              .contentType(contentType)
              .build());
    } catch (Exception e) {
      throw new ObjectStoreException("MinIO upload failed for id=" + id, e);
    }
    return objectKey;
  }

  private static String guessExtension(String contentType) {
    String ct = contentType.toLowerCase();
    if (ct.contains("png"))  return ".png";
    if (ct.contains("webp")) return ".webp";
    if (ct.contains("gif"))  return ".gif";
    return ".jpg";
  }
}
