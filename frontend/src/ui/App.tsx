import { useEffect, useMemo, useState } from 'react';
import type { EventRequest, EventType, PayloadSchema, UploadNotification } from './types';
import { clampSendCount } from './sendCounts';
import { shouldReportProgress } from './progress';
import { DataPayloadEditor, EventControls, ImageInputs, ResultPanel, StatusBanner } from './components';
import './App.css';

export default function App() {
  const [eventType, setEventType] = useState<EventType>('DATA');
  // Replaced on mount by the schema's example; '{}' avoids hardcoding payload keys here.
  const [jsonText, setJsonText] = useState<string>('{}');
  const [imageUrl, setImageUrl] = useState<string>('https://www.gstatic.com/webp/gallery/1.jpg');
  const [file, setFile] = useState<File | null>(null);
  const [result, setResult] = useState<string>('');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState<boolean>(false);
  const [uploadNotification, setUploadNotification] = useState<UploadNotification | null>(null);
  const [sendCount, setSendCount] = useState<number>(1);
  const [delaySeconds, setDelaySeconds] = useState<number>(0);
  const [allowedKeys, setAllowedKeys] = useState<string[]>([]);

  // /event-payload-schema.json is the backend schema staged into the image at build time.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const resp = await fetch('/event-payload-schema.json');
        if (!resp.ok) return;
        const schema: PayloadSchema = await resp.json();
        if (cancelled) return;
        const keys = Object.keys(schema.properties ?? {});
        if (keys.length) setAllowedKeys(keys);
        const sample = Array.isArray(schema.sample) ? schema.sample[0] : undefined;
        if (sample) setJsonText(JSON.stringify(sample, null, 2));
      } catch {
        // Keep the built-in fallback default when the schema asset is unavailable.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // The available send counts differ per event type, so a count valid for one type may be stale
  // after switching; clamp it back to the type's default rather than firing an invalid fan-out.
  useEffect(() => {
    setSendCount((current) => clampSendCount(eventType, current));
  }, [eventType]);

  const canSubmit = useMemo(() => {
    if (eventType === 'DATA') return true;
    if (eventType === 'IMAGE') return Boolean(imageUrl.trim()) || Boolean(file);
    return false;
  }, [eventType, imageUrl, file]);

  const isFileUpload = eventType === 'IMAGE' && Boolean(file);
  const submitLabel = busy ? (isFileUpload ? 'Uploading…' : 'Sending…') : isFileUpload ? 'Upload & send' : 'Send event';

  async function submit() {
    setBusy(true);
    setResult('');
    setErrorMessage(null);
    setUploadNotification(null);
    try {
      if (eventType === 'IMAGE' && file) {
        const fd = new FormData();
        fd.append('file', file);

        setUploadNotification({ variant: 'uploading', message: `Uploading ${file.name}…` });
        const resp = await fetch('/api/events/image-upload', {
          method: 'POST',
          body: fd,
        });

        const body = await resp.text();
        if (!resp.ok) {
          setUploadNotification({ variant: 'error', message: `Upload failed: ${body}` });
          return;
        }
        setUploadNotification({ variant: 'success', message: `Uploaded successfully` });
        setResult(body);
        return;
      }

      const req: EventRequest = { eventType };
      if (eventType === 'DATA') {
        req.payload = JSON.parse(jsonText);
      }
      if (eventType === 'IMAGE' && imageUrl.trim()) {
        req.imageUrl = imageUrl.trim();
      }

      for (let i = 0; i < sendCount; i++) {
        const resp = await fetch('/api/events', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(req),
        });

        const body = await resp.text();
        if (!resp.ok) throw new Error(body);
        const sent = i + 1;
        if (shouldReportProgress(sent, sendCount)) {
          setResult(sendCount === 1 ? body : `Sent ${sent}/${sendCount}`);
        }

        if (delaySeconds > 0 && i < sendCount - 1) {
          await new Promise((resolve) => setTimeout(resolve, delaySeconds * 1000));
        }
      }
    } catch (e) {
      setErrorMessage(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className='page'>
      <h1 className='page__heading'>Real-time Pipeline UI - Tester</h1>
      <p className='page__intro'>
        Submit <b>DATA</b> or <b>IMAGE</b> events. The backend publishes to Kafka; Flink processes into Postgres / MinIO.
      </p>

      <EventControls
        eventType={eventType}
        onEventTypeChange={setEventType}
        sendCount={sendCount}
        onSendCountChange={setSendCount}
        sendCountLocked={isFileUpload}
        delaySeconds={delaySeconds}
        onDelayChange={setDelaySeconds}
        submitDisabled={!canSubmit || busy}
        submitLabel={submitLabel}
        onSubmit={submit}
      />

      {eventType === 'DATA' && <DataPayloadEditor allowedKeys={allowedKeys} jsonText={jsonText} onChange={setJsonText} />}

      {eventType === 'IMAGE' && (
        <ImageInputs
          imageUrl={imageUrl}
          onImageUrlChange={setImageUrl}
          hasFile={Boolean(file)}
          onFileChange={(next) => {
            setFile(next);
            setUploadNotification(null);
          }}
          notification={uploadNotification}
        />
      )}

      {errorMessage && (
        <StatusBanner variant='error' alert className='section'>
          ⚠ {errorMessage}
        </StatusBanner>
      )}

      <ResultPanel result={result} />

      <div className='footer'>
        UI is served via Nginx on <code>http://localhost:3030</code>. Kafka UI on <code>http://localhost:8088</code>.
      </div>
    </div>
  );
}
