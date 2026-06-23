package com.webcharm.pipeline.userkeys.types;

/**
 * The pipeline stage a dead-letter record originated from, carried on every DlqRecord. The userKeys
 * job dead-letters only at parse: malformed or invalid events are diverted before windowing, while
 * the exactly-once XA sink commits whole checkpoints atomically and so surfaces its failures as job
 * restarts rather than per-row dead letters.
 */
public enum DlqStage {
  PARSE
}
