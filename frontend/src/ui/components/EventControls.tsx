import type { EventType } from '../types';
import { SEND_COUNTS } from '../sendCounts';

export function EventControls({
  eventType,
  onEventTypeChange,
  sendCount,
  onSendCountChange,
  sendCountLocked,
  delaySeconds,
  onDelayChange,
  submitDisabled,
  submitLabel,
  onSubmit,
}: {
  eventType: EventType;
  onEventTypeChange: (type: EventType) => void;
  sendCount: number;
  onSendCountChange: (count: number) => void;
  sendCountLocked: boolean;
  delaySeconds: number;
  onDelayChange: (seconds: number) => void;
  submitDisabled: boolean;
  submitLabel: string;
  onSubmit: () => void;
}) {
  return (
    <div className='controls'>
      <label>
        Event type:{' '}
        <select value={eventType} onChange={(e) => onEventTypeChange(e.target.value as EventType)}>
          <option value='DATA'>DATA</option>
          <option value='IMAGE'>IMAGE</option>
        </select>
      </label>

      <label>
        Send:{' '}
        <select value={String(sendCount)} onChange={(e) => onSendCountChange(Number(e.target.value))} disabled={sendCountLocked}>
          {SEND_COUNTS[eventType].map((count) => (
            <option key={count} value={String(count)}>
              {count.toLocaleString('en-US')}
            </option>
          ))}
        </select>
      </label>

      {sendCount > 1 && (
        <label>
          Delay (s):{' '}
          <input
            type='number'
            min={0}
            max={60}
            value={delaySeconds}
            onChange={(e) => onDelayChange(Math.min(60, Math.max(0, Number(e.target.value))))}
            className='delay-input'
          />
        </label>
      )}

      <button disabled={submitDisabled} onClick={onSubmit}>
        {submitLabel}
      </button>
    </div>
  );
}
