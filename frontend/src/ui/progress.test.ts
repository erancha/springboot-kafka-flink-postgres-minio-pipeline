import { describe, it, expect } from 'vitest';
import { PROGRESS_REPORT_INTERVAL, shouldReportProgress } from './progress';

describe('shouldReportProgress', () => {
  it('always reports the final send', () => {
    expect(shouldReportProgress(10, 10)).toBe(true);
    expect(shouldReportProgress(100000, 100000)).toBe(true);
  });

  it('reports on the fixed cadence', () => {
    expect(shouldReportProgress(PROGRESS_REPORT_INTERVAL, 100000)).toBe(true);
    expect(shouldReportProgress(PROGRESS_REPORT_INTERVAL * 2, 100000)).toBe(true);
  });

  it('stays silent between cadence points so renders do not fire every send', () => {
    expect(shouldReportProgress(1, 100000)).toBe(false);
    expect(shouldReportProgress(PROGRESS_REPORT_INTERVAL + 1, 100000)).toBe(false);
  });
});
