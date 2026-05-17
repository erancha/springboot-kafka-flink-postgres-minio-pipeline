package com.webcharm.pipeline.types;

import java.time.Instant;

/** A failed event for the dead-letter queue: the raw event, the error description, and when it failed. */
public record DlqRecord(String raw, String error, Instant failedAt) {}
