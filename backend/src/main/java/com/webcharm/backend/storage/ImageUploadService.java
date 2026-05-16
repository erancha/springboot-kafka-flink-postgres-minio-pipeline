package com.webcharm.backend.storage;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import org.springframework.web.multipart.MultipartFile;

/** Uploads an image file and returns its storage object key. */
public interface ImageUploadService {
  String upload(UUID id, Instant eventTime, MultipartFile file) throws IOException;
}
