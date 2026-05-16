package com.webcharm.backend.api;

import com.webcharm.backend.kafka.EventProducer;
import com.webcharm.backend.storage.ImageUploadService;
import com.webcharm.backend.model.EventRequest;
import com.webcharm.backend.model.EventResponse;
import jakarta.validation.Valid;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private final EventProducer eventProducer;
  private final ImageUploadService imageUploadService;

  public EventController(EventProducer eventProducer, ImageUploadService imageUploadService) {
    this.eventProducer = eventProducer;
    this.imageUploadService = imageUploadService;
  }

  /** Validates and publishes a typed event to Kafka; returns the assigned id and timestamp. */
  @PostMapping("/events")
  public EventResponse publishEvent(@Valid @RequestBody EventRequest request) {
    UUID id = UUID.randomUUID();
    Instant now = Instant.now();
    eventProducer.send(request.toEventMap(id, now, "ui"));
    return new EventResponse(id.toString(), now.toString());
  }

  /** Uploads the file to object storage, then publishes a pointer event to Kafka; returns the assigned id and timestamp. */
  @PostMapping(path = "/events/image-upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public EventResponse publishImageUpload(@RequestParam("file") MultipartFile file) throws IOException {
    if (file.isEmpty()) {
      throw new IllegalArgumentException("file is required");
    }
    UUID id = UUID.randomUUID();
    Instant now = Instant.now();
    log.info("image upload received: id={} filename={} contentType={} size={}B",
        id, file.getOriginalFilename(), file.getContentType(), file.getSize());
    String objectKey = imageUploadService.upload(id, now, file);
    eventProducer.send(Map.of(
        "id", id.toString(),
        "eventType", "IMAGE",
        "eventTime", now.toString(),
        "source", "ui",
        "imageObjectKey", objectKey));
    return new EventResponse(id.toString(), now.toString());
  }
}
