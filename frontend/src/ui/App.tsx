import React, { useEffect, useMemo, useState } from 'react';

type EventType = 'DATA' | 'IMAGE';

export default function App() {
  const [eventType, setEventType] = useState<EventType>('DATA');
  const [jsonText, setJsonText] = useState<string>(JSON.stringify({ foo: 'bar', count: 1 }, null, 2));
  const [imageUrl, setImageUrl] = useState<string>('https://picsum.photos/200');
  const [file, setFile] = useState<File | null>(null);
  const [result, setResult] = useState<string>('');
  const [busy, setBusy] = useState<boolean>(false);
  const [sendCount, setSendCount] = useState<number>(1);

  useEffect(() => {
    if (eventType === 'IMAGE') {
      if (sendCount === 1 || sendCount === 10 || sendCount === 100 || sendCount === 1000) return;
      setSendCount(1);
    } else if (eventType === 'DATA') {
      if (sendCount === 1 || sendCount === 10 || sendCount === 100 || sendCount === 1000 || sendCount === 10000 || sendCount === 100000) return;
      setSendCount(1);
    }
  }, [eventType, sendCount]);

  const canSubmit = useMemo(() => {
    if (eventType === 'DATA') return true;
    if (eventType === 'IMAGE') return Boolean(imageUrl.trim()) || Boolean(file);
    return false;
  }, [eventType, imageUrl, file]);

  async function submit() {
    setBusy(true);
    setResult('');
    try {
      if (eventType === 'IMAGE' && file) {
        const fd = new FormData();
        fd.append('file', file);

        const resp = await fetch('/api/events/image-upload', {
          method: 'POST',
          body: fd,
        });

        const body = await resp.text();
        if (!resp.ok) throw new Error(body);
        setResult(body);
        return;
      }

      let payload: unknown = undefined;
      if (eventType === 'DATA') {
        payload = JSON.parse(jsonText);
      }

      const req: any = {
        eventType,
      };

      if (eventType === 'DATA') {
        req.payload = payload;
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
        if (sendCount === 1) {
          setResult(body);
        } else {
          setResult(`Sent ${i + 1}/${sendCount}`);
        }
      }
    } catch (e: any) {
      setResult(String(e?.message ?? e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ fontFamily: 'system-ui, Arial', padding: 24, maxWidth: 900, margin: '0 auto' }}>
      <h1 style={{ margin: 0 }}>Real-time Pipeline UI</h1>
      <p style={{ color: '#444' }}>
        Submit <b>DATA</b> or <b>IMAGE</b> events. The backend publishes to Kafka; Flink processes into Postgres / MinIO.
      </p>

      <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginTop: 16 }}>
        <label>
          Event type:{' '}
          <select value={eventType} onChange={(e) => setEventType(e.target.value as EventType)}>
            <option value='DATA'>DATA</option>
            <option value='IMAGE'>IMAGE</option>
          </select>
        </label>

        <label>
          Send:{' '}
          <select value={String(sendCount)} onChange={(e) => setSendCount(Number(e.target.value))} disabled={eventType === 'IMAGE' && Boolean(file)}>
            <option value='1'>1</option>
            <option value='10'>10</option>
            <option value='100'>100</option>
            <option value='1000'>1,000</option>
            {eventType === 'DATA' && (
              <>
                <option value='10000'>10,000</option>
                <option value='100000'>100,000</option>
              </>
            )}
          </select>
        </label>

        <button disabled={!canSubmit || busy} onClick={submit}>
          {busy ? 'Sending…' : 'Send event'}
        </button>
      </div>

      {eventType === 'DATA' && (
        <div style={{ marginTop: 16 }}>
          <div style={{ fontWeight: 600, marginBottom: 8 }}>JSON payload</div>
          <textarea
            value={jsonText}
            onChange={(e) => setJsonText(e.target.value)}
            rows={12}
            style={{ width: '100%', fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas', fontSize: 13 }}
          />
        </div>
      )}

      {eventType === 'IMAGE' && (
        <div style={{ marginTop: 16, display: 'grid', gap: 10 }}>
          <label style={{ display: 'grid', gap: 6 }}>
            <div style={{ fontWeight: 600 }}>Image URL (optional)</div>
            <input value={imageUrl} onChange={(e) => setImageUrl(e.target.value)} placeholder='https://example.com/image.jpg' style={{ padding: 8 }} />
          </label>

          <label style={{ display: 'grid', gap: 6 }}>
            <div style={{ fontWeight: 600 }}>Upload file (optional)</div>
            <input type='file' accept='image/*' onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
          </label>

          <div style={{ color: '#555' }}>You can provide either URL or upload a file.</div>
        </div>
      )}

      <div style={{ marginTop: 16 }}>
        <div style={{ fontWeight: 600, marginBottom: 8 }}>Result</div>
        <pre style={{ background: '#f6f6f6', padding: 12, minHeight: 80, overflow: 'auto' }}>{result}</pre>
      </div>

      <div style={{ marginTop: 20, color: '#666', fontSize: 12 }}>
        UI is served via Nginx on <code>http://localhost:3030</code>. Kafka UI on <code>http://localhost:8088</code>.
      </div>
    </div>
  );
}
