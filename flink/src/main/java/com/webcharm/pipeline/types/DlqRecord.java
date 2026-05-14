package com.webcharm.pipeline.types;

import java.time.Instant;

/** Flink side-output record for Kafka messages that cannot be parsed by ParseEventFunction. */
public record DlqRecord(String raw, String error, Instant failedAt) {}
