package com.webcharm.backend.storage;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import org.springframework.web.multipart.MultipartFile;

/** Stores and removes image objects in the backing object store. */
public interface ImageUploadService {
  String upload(UUID id, Instant eventTime, MultipartFile file) throws IOException;

  /** Removes a stored object, used to compensate an upload whose follow-up publish failed. */
  void delete(String objectKey);
}
