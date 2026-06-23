package com.webcharm.pipeline.userkeys.types;

import java.time.Instant;

/** A failed event diverted to the dead-letter queue, tagged with the stage that rejected it. */
public record DlqRecord(DlqStage stage, String raw, String error, Instant failedAt) {}
