package com.webcharm.contract.eventtype.event;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES-GCM cipher for DATA payloads, shared by the encrypting producer and any decrypting consumer
 * so both directions run one implementation and cannot drift. The envelope is a JSON object holding
 * the algorithm label, per-message nonce, and ciphertext.
 *
 * Pure JDK (javax.crypto only) to honor the contract module's no-Spring, no-Jackson rule, so it can
 * be bundled into both runtimes.
 */
public final class PayloadCipher {

  public static final String KEY_ALGORITHM = "AES";

  private static final String TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int GCM_TAG_BITS = 128;
  private static final int GCM_IV_BYTES = 12;

  private static final String ALG = "alg";
  private static final String IV = "iv";                 // base64 GCM nonce, unique per message
  private static final String CIPHERTEXT = "ciphertext"; // base64 of ciphertext concatenated with the GCM tag
  private static final String ALG_VALUE = "AES-GCM";

  private final SecretKey key;
  private final SecureRandom random = new SecureRandom();

  public PayloadCipher(SecretKey key) {
    this.key = key;
  }

  /**
   * Decodes a base64 AES key into the SecretKey this cipher consumes, shared so the producer and a
   * consumer derive the key identically.
   *
   * @throws IllegalArgumentException if the decoded key is not 16, 24, or 32 bytes
   */
  public static SecretKey keyFromBase64(String base64Key) {
    byte[] raw = Base64.getDecoder().decode(base64Key.trim());
    if (raw.length != 16 && raw.length != 24 && raw.length != 32) {
      throw new IllegalArgumentException(
          "AES key must decode to 16, 24, or 32 bytes; got " + raw.length);
    }
    return new SecretKeySpec(raw, KEY_ALGORITHM);
  }

  /** Encrypts plaintext under a fresh per-message nonce, returning the envelope. */
  public Map<String, Object> encrypt(byte[] plaintext) {
    try {
      byte[] iv = new byte[GCM_IV_BYTES];
      random.nextBytes(iv);
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
      byte[] ciphertext = cipher.doFinal(plaintext);

      Map<String, Object> envelope = new LinkedHashMap<>();
      envelope.put(ALG, ALG_VALUE);
      envelope.put(IV, Base64.getEncoder().encodeToString(iv));
      envelope.put(CIPHERTEXT, Base64.getEncoder().encodeToString(ciphertext));
      return envelope;
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("payload encryption failed", e);
    }
  }

  /**
   * Recovers the plaintext from an envelope produced by encrypt.
   *
   * @throws IllegalStateException if the envelope was tampered with (GCM authentication failure) or
   *     its fields are malformed
   */
  public byte[] decrypt(Map<String, Object> envelope) {
    try {
      byte[] iv = Base64.getDecoder().decode((String) envelope.get(IV));
      byte[] ciphertext = Base64.getDecoder().decode((String) envelope.get(CIPHERTEXT));
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
      return cipher.doFinal(ciphertext);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("payload decryption failed", e);
    }
  }
}
