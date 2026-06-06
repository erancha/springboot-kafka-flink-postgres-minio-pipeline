export type EventType = 'DATA' | 'IMAGE';

// Mirrors the backend EventRequest contract: payload carries the DATA JSON, imageUrl carries the
// source URL for IMAGE-by-URL events. Exactly one is populated per request.
export interface EventRequest {
  eventType: EventType;
  payload?: unknown;
  imageUrl?: string;
}

// The subset of the JSON Schema served at /event-payload-schema.json that the UI consumes:
// property names drive the "allowed keys" hint; examples[0] seeds the payload editor.
export interface PayloadSchema {
  properties?: Record<string, unknown>;
  examples?: unknown[];
}

export type BannerVariant = 'uploading' | 'success' | 'error';

export interface UploadNotification {
  variant: BannerVariant;
  message: string;
}
