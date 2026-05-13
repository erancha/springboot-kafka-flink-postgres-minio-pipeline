package com.webcharm.backend.api;

import com.webcharm.backend.kafka.EventProducer;
import com.webcharm.backend.model.EventRequest;
import com.webcharm.backend.model.EventResponse;
import jakarta.validation.Valid;
import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api")
public class EventController {
  private final EventProducer eventProducer;

  public EventController(EventProducer eventProducer) {
    this.eventProducer = eventProducer;
  }

  @PostMapping("/events")
  public EventResponse publishEvent(@Valid @RequestBody EventRequest request) {
    UUID id = UUID.randomUUID();
    Instant now = Instant.now();

    Map<String, Object> event = request.toEventMap(id, now, "ui");
    eventProducer.send(event);

    return new EventResponse(id.toString(), now.toString());
  }

  @PostMapping(path = "/events/image-upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public EventResponse publishImageUpload(@RequestParam("file") MultipartFile file) throws IOException {
    if (file.isEmpty()) {
      throw new IllegalArgumentException("file is required");
    }

    UUID id = UUID.randomUUID();
    Instant now = Instant.now();

    String base64 = Base64.getEncoder().encodeToString(file.getBytes());

    Map<String, Object> event = Map.of(
        "id", id.toString(),
        "eventType", "IMAGE",
        "eventTime", now.toString(),
        "source", "ui",
        "imageBase64", base64,
        "imageContentType", file.getContentType() == null ? "application/octet-stream" : file.getContentType());

    eventProducer.send(event);

    return new EventResponse(id.toString(), now.toString());
  }
}
