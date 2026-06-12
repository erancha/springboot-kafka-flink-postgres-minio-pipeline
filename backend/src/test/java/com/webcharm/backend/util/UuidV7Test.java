package com.webcharm.backend.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Verifies the UUIDv7 layout (version, variant), its time-ordered prefix, and uniqueness. */
class UuidV7Test {

  /** The 48-bit millisecond timestamp occupies the high bits of the most significant long. */
  private static long timestampMillis(UUID u) {
    return u.getMostSignificantBits() >>> 16;
  }

  @Test
  void carriesVersion7AndIetfVariant() {
    UUID u = UuidV7.next();
    assertThat(u.version()).isEqualTo(7);
    assertThat(u.variant()).isEqualTo(2);
  }

  @Test
  void timestampPrefixMatchesWallClock() {
    long before = System.currentTimeMillis();
    long prefix = timestampMillis(UuidV7.next());
    long after = System.currentTimeMillis();
    assertThat(prefix).isBetween(before, after);
  }

  @Test
  void timestampIsNonDecreasingAcrossTime() throws InterruptedException {
    long first = timestampMillis(UuidV7.next());
    Thread.sleep(3);
    long second = timestampMillis(UuidV7.next());
    assertThat(second).isGreaterThanOrEqualTo(first);
  }

  @Test
  void sameMillisecondIdsStayUnique() {
    Set<UUID> ids = new HashSet<>();
    for (int i = 0; i < 10_000; i++) {
      ids.add(UuidV7.next());
    }
    assertThat(ids).hasSize(10_000);
  }
}
