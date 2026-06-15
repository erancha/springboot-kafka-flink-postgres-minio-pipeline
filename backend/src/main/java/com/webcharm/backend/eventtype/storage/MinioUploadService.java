package com.webcharm.backend.eventtype.storage;

import com.webcharm.backend.storage.ObjectStoreException;
import com.webcharm.contract.eventtype.image.ImageFormat;
import com.webcharm.contract.eventtype.image.ImageObjectKey;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

/** Stores multipart images in MinIO and removes them by object key. */
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
   * Streams from file.getInputStream() without buffering the body in heap.
   * Throws ObjectStoreException if the MinIO call fails.
   * Lets IOException from file.getInputStream() propagate (mapped to 500 by GlobalExceptionHandler).
   */
  public String upload(UUID id, Instant eventTime, MultipartFile file) throws IOException {
    String contentType = file.getContentType() != null ? file.getContentType() : "image/jpeg";
    String objectKey = ImageObjectKey.of(
        eventTime.atZone(ZoneOffset.UTC).toLocalDate(), id, ImageFormat.extensionForContentType(contentType));

    // getInputStream() IOException propagates directly (stream open failure is not a MinIO error).
    // All putObject exceptions are wrapped as ObjectStoreException (MinIO failure).
    try (InputStream imageStream = file.getInputStream()) {
      try {
        minioClient.putObject(
            PutObjectArgs.builder()
                .bucket(bucket).object(objectKey)
                .stream(imageStream, file.getSize(), -1)
                .contentType(contentType)
                .build());
      } catch (Exception e) {
        throw new ObjectStoreException("MinIO upload failed for id=" + id, e);
      }
    }
    return objectKey;
  }

  @Override
  public void delete(String objectKey) {
    try {
      minioClient.removeObject(
          RemoveObjectArgs.builder().bucket(bucket).object(objectKey).build());
    } catch (Exception e) {
      throw new ObjectStoreException("MinIO delete failed for objectKey=" + objectKey, e);
    }
  }
}
