package com.webcharm.pipeline.common.dlq;

import java.time.Instant;

/**
 * A failed event diverted to the dead-letter queue: the originating stage, the raw event, the error
 * description, and when it failed.
 *
 * stage is the enum-constant name of the producing pipeline's DlqStage, carried as a String so this
 * record stays independent of any one pipeline's stage set. Jackson serializes a DlqStage enum to
 * exactly this name, so the dead-letter JSON wire format is identical to a typed-enum field.
 */
public record DlqRecord(String stage, String raw, String error, Instant failedAt) {}
