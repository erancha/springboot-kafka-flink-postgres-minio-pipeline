import { describe, it, expect } from 'vitest';
import { SEND_COUNTS, clampSendCount } from './sendCounts';

describe('SEND_COUNTS', () => {
  it('offers higher fan-out for DATA than for IMAGE', () => {
    expect(SEND_COUNTS.DATA).toEqual([1, 10, 100, 1000, 10000, 100000]);
    expect(SEND_COUNTS.IMAGE).toEqual([1, 10, 100, 1000]);
  });
});

describe('clampSendCount', () => {
  it('keeps a count that is valid for the event type', () => {
    expect(clampSendCount('DATA', 100000)).toBe(100000);
    expect(clampSendCount('IMAGE', 1000)).toBe(1000);
  });

  it('resets to 1 when the count is not valid for the event type', () => {
    expect(clampSendCount('IMAGE', 100000)).toBe(1);
  });
});
