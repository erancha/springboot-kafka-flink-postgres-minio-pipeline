import type { UploadNotification } from '../types';
import { StatusBanner } from './StatusBanner';

export function ImageInputs({
  imageUrl,
  onImageUrlChange,
  hasFile,
  onFileChange,
  notification,
}: {
  imageUrl: string;
  onImageUrlChange: (url: string) => void;
  hasFile: boolean;
  onFileChange: (file: File | null) => void;
  notification: UploadNotification | null;
}) {
  return (
    <div className='image-section'>
      <label className='field'>
        <div className='field__label'>Image URL (optional)</div>
        <input value={imageUrl} onChange={(e) => onImageUrlChange(e.target.value)} placeholder='https://example.com/image.jpg' className='url-input' />
      </label>

      <label className='field'>
        <div className='field__label'>Upload file (optional)</div>
        <input type='file' accept='image/*' onChange={(e) => onFileChange(e.target.files?.[0] ?? null)} />
      </label>

      <div className='helper-text'>
        {hasFile ? (
          <span>
            File will be uploaded when you click <b>Upload &amp; send</b>.
          </span>
        ) : (
          'You can provide either URL or upload a file.'
        )}
      </div>

      {notification && <StatusBanner variant={notification.variant}>{notification.message}</StatusBanner>}
    </div>
  );
}
