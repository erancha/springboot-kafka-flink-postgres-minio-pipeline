package com.webcharm.pipeline.types;

/** The pipeline stage a dead-letter record originated from, carried on every DlqRecord. */
public enum DlqStage {
  PARSE,
  IMAGE_ENRICH,
  IMAGE_POSTGRES,
  DATA_POSTGRES,
  COUNT_POSTGRES
}
