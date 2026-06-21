package com.webcharm.contract.eventtype.event;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import javax.crypto.SecretKey;
import org.junit.jupiter.api.Test;

/**
 * Verifies the shared AES-GCM cipher: ciphertext round-trips to the original bytes, the envelope
 * hides the plaintext, each message gets a fresh nonce, and the key decoder rejects wrong lengths.
 */
class PayloadCipherTest {

  private static final SecretKey KEY =
      PayloadCipher.keyFromBase64(Base64.getEncoder().encodeToString(new byte[32]));

  @Test
  void encryptThenDecryptRecoversPlaintext() {
    PayloadCipher cipher = new PayloadCipher(KEY);
    byte[] plaintext = "{\"sku\":\"WIDGET\"}".getBytes(StandardCharsets.UTF_8);

    Map<String, Object> envelope = cipher.encrypt(plaintext);

    assertArrayEquals(plaintext, cipher.decrypt(envelope));
  }

  @Test
  void envelopeDoesNotExposePlaintext() {
    PayloadCipher cipher = new PayloadCipher(KEY);

    Map<String, Object> envelope = cipher.encrypt("WIDGET".getBytes(StandardCharsets.UTF_8));

    assertFalse(envelope.toString().contains("WIDGET"));
  }

  @Test
  void usesAFreshNoncePerMessage() {
    PayloadCipher cipher = new PayloadCipher(KEY);
    byte[] plaintext = "same".getBytes(StandardCharsets.UTF_8);

    Map<String, Object> a = cipher.encrypt(plaintext);
    Map<String, Object> b = cipher.encrypt(plaintext);

    // Reusing a nonce under one key is a fatal GCM misuse; nonces must differ.
    assertNotEquals(a.get("iv"), b.get("iv"));
  }

  @Test
  void keyFromBase64RejectsWrongLength() {
    String shortKey = Base64.getEncoder().encodeToString(new byte[10]);
    assertThrows(IllegalArgumentException.class, () -> PayloadCipher.keyFromBase64(shortKey));
  }

  @Test
  void keyFromBase64AcceptsValidAesLengths() {
    assertEquals("AES",
        PayloadCipher.keyFromBase64(Base64.getEncoder().encodeToString(new byte[16])).getAlgorithm());
    assertEquals("AES",
        PayloadCipher.keyFromBase64(Base64.getEncoder().encodeToString(new byte[32])).getAlgorithm());
  }
}
