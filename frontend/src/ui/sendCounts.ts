import type { EventType } from './types';

// Fan-out multipliers offered in the "Send" dropdown, per event type. IMAGE caps lower than DATA
// because each IMAGE event fetches and stores a binary, so high-volume fan-out is not offered.
// First entry is the default and the fallback used when an out-of-range count is clamped.
export const SEND_COUNTS: Record<EventType, number[]> = {
  DATA: [1, 10, 100, 1000, 10000, 100000],
  IMAGE: [1, 10, 100, 1000],
};

export function clampSendCount(eventType: EventType, count: number): number {
  const allowed = SEND_COUNTS[eventType];
  return allowed.includes(count) ? count : allowed[0];
}
