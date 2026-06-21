package com.webcharm.backend.eventtype.crypto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.contract.eventtype.event.EventFields;
import com.webcharm.contract.eventtype.event.PayloadCipher;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Encrypts DATA event payloads at the ingestion edge with AES-GCM so DATA contents are never
 * readable on the Kafka wire or at rest.
 *
 * Encryption is gated by the configured key: with a key present payloads are encrypted, with none
 * the service is a pass-through, so callers invoke it unconditionally.
 */
@Component
public class PayloadEncryptionService {

  private static final Logger log = LoggerFactory.getLogger(PayloadEncryptionService.class);

  // Null when no key is configured, which leaves encryption off.
  private final PayloadCipher cipher;
  private final ObjectMapper objectMapper;

  public PayloadEncryptionService(
      @Value("${app.payload.encryption.key:}") String base64Key,
      ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    if (base64Key.isBlank()) {
      this.cipher = null;
      log.warn("PAYLOAD_ENCRYPTION_KEY is unset — DATA payloads are published unencrypted.");
    } else {
      this.cipher = new PayloadCipher(PayloadCipher.keyFromBase64(base64Key));
    }
  }

  /**
   * Replaces the event's payload with its encrypted envelope in place. A no-op when no key is
   * configured or the event carries no payload (IMAGE events), so it is safe to call unconditionally.
   */
  public void encryptPayload(Map<String, Object> event) {
    if (cipher == null) {
      return;
    }
    Object payload = event.get(EventFields.PAYLOAD);
    if (payload == null) {
      return;
    }
    try {
      event.put(EventFields.PAYLOAD, cipher.encrypt(objectMapper.writeValueAsBytes(payload)));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("failed to serialize payload for encryption", e);
    }
  }
}
