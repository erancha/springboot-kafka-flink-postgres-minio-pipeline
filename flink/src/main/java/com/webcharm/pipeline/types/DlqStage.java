package com.webcharm.pipeline.types;

/** The pipeline stage a dead-letter record originated from, carried on every DlqRecord. */
public enum DlqStage {
  PARSE,
  IMAGE_ENRICH,
  IMAGE_POSTGRES,
  DATA_POSTGRES,
  EVENT_TYPE_COUNT_POSTGRES,
  IMAGE_SIZE_BUCKET_COUNT_POSTGRES
}
