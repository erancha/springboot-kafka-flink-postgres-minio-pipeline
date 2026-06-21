package com.webcharm.backend.eventtype.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webcharm.contract.eventtype.event.EventFields;
import com.webcharm.contract.eventtype.event.PayloadCipher;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies the backend encryption service: with a key it replaces a DATA payload with an envelope
 * that hides the plaintext yet decrypts back through the shared cipher; with no key, and for IMAGE
 * events that carry no payload, the event is left untouched.
 */
class PayloadEncryptionServiceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  // Fixed AES-256 key so the test does not depend on a generated one.
  private static final String KEY = Base64.getEncoder().encodeToString(new byte[32]);

  // With a key set, the payload is replaced by an envelope that hides the plaintext yet decrypts
  // back to the original through the shared cipher.
  @Test
  void encryptsPayloadIntoEnvelopeThatDecryptsBack() throws Exception {
    PayloadEncryptionService service = new PayloadEncryptionService(KEY, MAPPER);
    Map<String, Object> event = new HashMap<>();
    event.put(EventFields.PAYLOAD, Map.of("orderNumber", "A-100", "sku", "WIDGET"));

    service.encryptPayload(event);

    @SuppressWarnings("unchecked")
    Map<String, Object> envelope = (Map<String, Object>) event.get(EventFields.PAYLOAD);
    assertThat(MAPPER.writeValueAsString(envelope)).doesNotContain("orderNumber", "WIDGET", "A-100");

    PayloadCipher cipher = new PayloadCipher(PayloadCipher.keyFromBase64(KEY));
    Map<String, Object> recovered =
        MAPPER.readValue(cipher.decrypt(envelope), new TypeReference<Map<String, Object>>() {});
    assertThat(recovered).containsEntry("orderNumber", "A-100").containsEntry("sku", "WIDGET");
  }

  // With no key configured, the payload passes through unencrypted.
  @Test
  void noKeyLeavesPayloadUntouched() {
    PayloadEncryptionService service = new PayloadEncryptionService("", MAPPER);
    Map<String, Object> original = Map.of("orderNumber", "A-100");
    Map<String, Object> event = new HashMap<>();
    event.put(EventFields.PAYLOAD, original);

    service.encryptPayload(event);

    assertThat(event.get(EventFields.PAYLOAD)).isEqualTo(original);
  }

  // An IMAGE event has no payload field, so even with a key the event is left unchanged.
  @Test
  void leavesImageEventsWithoutPayloadUntouched() {
    PayloadEncryptionService service = new PayloadEncryptionService(KEY, MAPPER);
    Map<String, Object> event = new HashMap<>();
    event.put(EventFields.IMAGE_OBJECT_KEY, "images/2026/x.png");

    service.encryptPayload(event);

    assertThat(event).doesNotContainKey(EventFields.PAYLOAD);
    assertThat(event).containsEntry(EventFields.IMAGE_OBJECT_KEY, "images/2026/x.png");
  }

  // A configured key that decodes to a non-AES length fails fast at construction.
  @Test
  void rejectsKeyOfWrongLength() {
    String shortKey = Base64.getEncoder().encodeToString(new byte[10]);
    assertThatThrownBy(() -> new PayloadEncryptionService(shortKey, MAPPER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("16, 24, or 32 bytes");
  }
}
