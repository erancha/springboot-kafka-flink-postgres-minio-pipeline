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

/** Stores multipart images in the object store and removes them by object key. */
@Service
public class ObjectStoreUploadService implements ImageUploadService {

  private final MinioClient objectStoreClient;
  private final String bucket;

  public ObjectStoreUploadService(MinioClient objectStoreClient, @Value("${object-store.bucket}") String bucket) {
    this.objectStoreClient = objectStoreClient;
    this.bucket = bucket;
  }

  /**
   * Uploads file bytes to the object store under key images/{date}/{id}.{ext} and returns that key, streaming
   * the multipart body rather than buffering it in heap.
   *
   * @throws ObjectStoreException if the object-store upload fails
   * @throws IOException if opening the multipart stream fails
   */
  public String upload(UUID id, Instant eventTime, MultipartFile file) throws IOException {
    String contentType = file.getContentType() != null ? file.getContentType() : "image/jpeg";
    String objectKey = ImageObjectKey.of(
        eventTime.atZone(ZoneOffset.UTC).toLocalDate(), id, ImageFormat.extensionForContentType(contentType));

    try (InputStream imageStream = file.getInputStream()) {
      try {
        objectStoreClient.putObject(
            PutObjectArgs.builder()
                .bucket(bucket).object(objectKey)
                .stream(imageStream, file.getSize(), -1)
                .contentType(contentType)
                .build());
      } catch (Exception e) {
        throw new ObjectStoreException("object-store upload failed for id=" + id, e);
      }
    }
    return objectKey;
  }

  @Override
  public void delete(String objectKey) {
    try {
      objectStoreClient.removeObject(
          RemoveObjectArgs.builder().bucket(bucket).object(objectKey).build());
    } catch (Exception e) {
      throw new ObjectStoreException("object-store delete failed for objectKey=" + objectKey, e);
    }
  }
}
