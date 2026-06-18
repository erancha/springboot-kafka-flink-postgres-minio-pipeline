// Large fan-out sends (up to 100000) would otherwise call setResult on every iteration, forcing a
// React re-render per request. Reporting only on a fixed cadence and the final send keeps the
// progress readout responsive without rendering tens of thousands of times.
export const PROGRESS_REPORT_INTERVAL = 1000;

// sent is the 1-based count of completed sends; total is the requested fan-out size.
export function shouldReportProgress(sent: number, total: number): boolean {
  return sent === total || sent % PROGRESS_REPORT_INTERVAL === 0;
}
