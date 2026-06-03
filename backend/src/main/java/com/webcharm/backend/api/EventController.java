package com.webcharm.backend.api;

import com.webcharm.backend.kafka.EventProducer;
import com.webcharm.backend.storage.ImageUploadService;
import com.webcharm.backend.model.EventRequest;
import com.webcharm.backend.model.EventResponse;
import com.webcharm.backend.model.EventType;
import jakarta.validation.Valid;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/** REST controller that publishes events to Kafka; delegates object storage uploads to ImageUploadService for file-upload requests. */
@RestController
@RequestMapping("/api")
public class EventController {

  private static final Logger log = LoggerFactory.getLogger(EventController.class);

  // Cap on stored image bytes; below the 20 MB Spring multipart limit, which only bounds heap.
  private static final long MAX_IMAGE_BYTES = 10L * 1024 * 1024;

  @Value("${IMAGE_URL_ALLOWED_HOSTS:}")
  private String allowedImageHosts;

  private final EventProducer eventProducer;
  private final ImageUploadService imageUploadService;

  public EventController(EventProducer eventProducer, ImageUploadService imageUploadService) {
    this.eventProducer = eventProducer;
    this.imageUploadService = imageUploadService;
  }

  @PostConstruct
  void init() {
    if (allowedImageHosts.isBlank()) {
      log.warn("IMAGE_URL_ALLOWED_HOSTS is not configured — all imageUrl submissions will be DENIED.");
    }
  }

  /** Validates and publishes a typed event to Kafka; returns the assigned id and timestamp. */
  @PostMapping("/events")
  public EventResponse publishEvent(@Valid @RequestBody EventRequest request) {
    if (EventType.IMAGE == request.getEventType()) {
      validateImageUrl(request.getImageUrl());
    }
    UUID id = UUID.randomUUID();
    Instant now = Instant.now();
    eventProducer.send(request.toEventMap(id, now, "ui"));
    return new EventResponse(id.toString(), now.toString());
  }

  /**
   * Validates the imageUrl for an IMAGE event: it must be present and a well-formed http(s) URL
   * whose host is in IMAGE_URL_ALLOWED_HOSTS. Guards against SSRF. An empty allowlist denies all
   * URLs — the env var must be explicitly configured to permit any fetch. File uploads use the
   * separate /api/events/image-upload endpoint and do not pass through here.
   *
   * @throws IllegalArgumentException if the URL is null, blank, malformed, has no host, or uses a non-http(s) scheme
   * @throws SecurityException if the allowlist is unconfigured or the host is absent from the allowlist
   */
  private void validateImageUrl(String rawUrl) {
    if (rawUrl == null || rawUrl.isBlank()) {
      throw new IllegalArgumentException(
          "IMAGE event via /api/events requires a non-blank imageUrl; "
              + "use /api/events/image-upload for file uploads");
    }
    URI uri;
    try {
      uri = URI.create(rawUrl);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("imageUrl is not a valid URI: " + rawUrl, e);
    }
    String scheme = uri.getScheme();
    if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException("imageUrl scheme not allowed: " + scheme);
    }
    String host = uri.getHost();
    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException("imageUrl has no host");
    }
    if (allowedImageHosts.isBlank()) {
      throw new SecurityException("imageUrl fetch denied: host not permitted.");
    }
    boolean allowed = Arrays.stream(allowedImageHosts.split(","))
        .map(String::trim)
        .anyMatch(h -> h.equalsIgnoreCase(host));
    if (!allowed) {
      throw new SecurityException("imageUrl host not in allowlist: " + host);
    }
  }

  /**
   * Rejects uploads that are not storable images: empty bodies, a missing or non-image/*
   * Content-Type, or bodies above the size cap. Content-Type is client-supplied, so this bounds
   * what gets stored without authenticating the bytes.
   *
   * TODO: the backend (Spring Boot) and Flink (not Spring Boot) currently duplicate these image
   * rules; a module shared by both would enforce one definition instead of two parallel copies.
   */
  private void validateImageFile(MultipartFile file) {
    if (file.isEmpty()) {
      throw new IllegalArgumentException("file is required");
    }
    String contentType = file.getContentType();
    if (contentType == null || !contentType.toLowerCase().startsWith("image/")) {
      throw new IllegalArgumentException(
          "file must be an image (Content-Type="
              + (contentType == null ? "<absent>" : contentType) + ")");
    }
    if (file.getSize() > MAX_IMAGE_BYTES) {
      throw new IllegalArgumentException(
          "file exceeds " + (MAX_IMAGE_BYTES / 1024 / 1024) + " MB cap");
    }
  }

  /** Uploads the file to object storage, then publishes a pointer event to Kafka; returns the assigned id and timestamp. */
  @PostMapping(path = "/events/image-upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public EventResponse publishImageUpload(@RequestParam("file") MultipartFile file) throws IOException {
    validateImageFile(file);
    UUID id = UUID.randomUUID();
    Instant now = Instant.now();
    log.info("image upload received: id={} filename={} contentType={} size={}B",
        id, file.getOriginalFilename(), file.getContentType(), file.getSize());
    String objectKey = imageUploadService.upload(id, now, file);
    try {
      eventProducer.send(Map.of(
          "id", id.toString(),
          "eventType", "IMAGE",
          "eventTime", now.toString(),
          "source", "ui",
          "imageObjectKey", objectKey));
    } catch (RuntimeException publishFailure) {
      // Delete the now-orphaned object so a failed publish does not leak storage. Best-effort —
      // a failed cleanup must not mask the original publish failure.
      try {
        imageUploadService.delete(objectKey);
      } catch (RuntimeException cleanupFailure) {
        log.error("Orphaned object {} left after publish failure; cleanup also failed", objectKey,
            cleanupFailure);
      }
      throw publishFailure;
    }
    return new EventResponse(id.toString(), now.toString());
  }
}
