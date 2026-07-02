package com.webcharm.backend.util;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import java.util.UUID;

/**
 * Generates time-ordered UUIDv7 event ids, via java-uuid-generator.
 *
 * The 48-bit millisecond timestamp prefix keeps successive ids on the recent, cache-resident right
 * edge of the Postgres primary-key b-tree instead of scattering across it the way UUIDv4 does, so
 * insert cost stays flat as processed_events grows. The random tail keeps ids globally unique and
 * spreads same-millisecond inserts across leaf pages, easing right-edge lock contention under
 * concurrent sink writers.
 */
public final class UuidV7 {

  // The generator is thread-safe and carries no per-call state worth recreating, so one shared
  // instance serves every request.
  private static final NoArgGenerator GENERATOR = Generators.timeBasedEpochGenerator();

  private UuidV7() {}

  public static UUID next() {
    return GENERATOR.generate();
  }
}
