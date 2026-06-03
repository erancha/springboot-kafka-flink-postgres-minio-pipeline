package com.webcharm.pipeline.types;

import java.time.Instant;

/** A failed event for the dead-letter queue: the originating stage, the raw event, the error description, and when it failed. */
public record DlqRecord(DlqStage stage, String raw, String error, Instant failedAt) {}
