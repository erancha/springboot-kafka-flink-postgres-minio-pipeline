package com.webcharm.backend.eventtype.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

/** Validates a JSON object against a JSON Schema. */
@Component
public class PayloadSchemaValidator {

  private final ObjectMapper objectMapper;
  // Compiled schema, or null when EVENT_PAYLOAD_SCHEMA is unset (validation disabled).
  private final JsonSchema schema;

  public PayloadSchemaValidator(
      @Value("${EVENT_PAYLOAD_SCHEMA:}") String schemaLocation,
      ObjectMapper objectMapper,
      ResourceLoader resourceLoader) throws IOException {
    this.objectMapper = objectMapper;
    if (schemaLocation.isBlank()) {
      this.schema = null;
      return;
    }
    Resource resource = resourceLoader.getResource(schemaLocation);
    try (InputStream in = resource.getInputStream()) {
      JsonNode schemaNode = objectMapper.readTree(in);
      this.schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(schemaNode);
    }
  }

  /**
   * @throws IllegalArgumentException if a schema is configured and the payload violates it
   */
  public void validate(Map<String, Object> payload) {
    if (schema == null) {
      return;
    }
    JsonNode node = objectMapper.valueToTree(payload == null ? Map.of() : payload);
    Set<ValidationMessage> errors = schema.validate(node);
    if (!errors.isEmpty()) {
      throw new IllegalArgumentException("payload does not match schema: " + errors);
    }
  }
}
