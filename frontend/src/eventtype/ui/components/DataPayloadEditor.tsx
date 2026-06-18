export function DataPayloadEditor({
  allowedKeys,
  jsonText,
  onChange,
}: {
  allowedKeys: string[];
  jsonText: string;
  onChange: (text: string) => void;
}) {
  return (
    <div className='section'>
      <div className='section__label'>JSON payload</div>
      {allowedKeys.length > 0 && <div className='hint'>Allowed keys: {allowedKeys.join(', ')}</div>}
      <textarea value={jsonText} onChange={(e) => onChange(e.target.value)} rows={12} className='json-input' />
    </div>
  );
}
