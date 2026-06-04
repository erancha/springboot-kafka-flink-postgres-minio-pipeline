import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, vi, afterEach } from 'vitest';
import App from './App';

describe('canSubmit', () => {
  it('is true for DATA event — button is enabled by default', () => {
    render(<App />);
    expect(screen.getByRole('button', { name: /send event/i })).not.toBeDisabled();
  });

  it('is false for IMAGE event with no url and no file — button is disabled', async () => {
    render(<App />);
    const user = userEvent.setup();

    // Switch to IMAGE
    await user.selectOptions(screen.getAllByRole('combobox')[0], 'IMAGE');

    // Clear the default imageUrl value ('https://www.gstatic.com/webp/gallery/1.jpg')
    const urlInput = screen.getByPlaceholderText('https://example.com/image.jpg');
    await user.clear(urlInput);

    expect(screen.getByRole('button', { name: /send event/i })).toBeDisabled();
  });

  it('is true for IMAGE event with a non-empty imageUrl', async () => {
    render(<App />);
    const user = userEvent.setup();

    await user.selectOptions(screen.getAllByRole('combobox')[0], 'IMAGE');
    // Default imageUrl is 'https://www.gstatic.com/webp/gallery/1.jpg' (non-empty), so button is enabled
    expect(screen.getByRole('button', { name: /send event/i })).not.toBeDisabled();
  });
});

describe('delay between requests', () => {
  it('is hidden when sendCount is 1 (default)', () => {
    render(<App />);
    expect(screen.queryByLabelText(/delay \(s\)/i)).not.toBeInTheDocument();
  });

  it('appears with default value 0 when sendCount is greater than 1', async () => {
    render(<App />);
    const user = userEvent.setup();
    await user.selectOptions(screen.getAllByRole('combobox')[1], '10');
    const input = screen.getByLabelText(/delay \(s\)/i);
    expect(input).toBeInTheDocument();
    expect(input).toHaveValue(0);
  });
});

describe('DATA payload schema (loaded from /event-payload-schema.json)', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  // Assert against the mocked schema's keys, not the real ones, so the test survives schema changes.
  function stubSchemaFetch() {
    vi.stubGlobal('fetch', vi.fn((url: string) => {
      if (typeof url === 'string' && url.includes('event-payload-schema.json')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({
            properties: { alpha: { type: 'string' }, beta: { type: 'integer' } },
            sample: [{ alpha: 'x', beta: 7 }],
          }),
        });
      }
      return Promise.resolve({ ok: true, text: () => Promise.resolve('ok') });
    }));
  }

  it('lists the allowed payload keys from the schema', async () => {
    stubSchemaFetch();
    render(<App />);
    const hint = await screen.findByText(/allowed keys:/i);
    expect(hint).toHaveTextContent('alpha');
    expect(hint).toHaveTextContent('beta');
  });

  it("seeds the input with the schema's example payload", async () => {
    stubSchemaFetch();
    render(<App />);
    const textarea = await screen.findByDisplayValue(/alpha/);
    expect(textarea).toHaveDisplayValue(/"beta": 7/);
  });
});

describe('submit() error handling', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('shows error message on network failure', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('Network error')));
    render(<App />);
    const user = userEvent.setup();

    await user.click(screen.getByRole('button', { name: /send event/i }));

    // App catches the error and calls setResult(e.message), displayed in <pre>
    expect(await screen.findByText(/network error/i)).toBeInTheDocument();
  });

  it('shows error message on non-2xx response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      text: () => Promise.resolve('Internal Server Error'),
    }));
    render(<App />);
    const user = userEvent.setup();

    await user.click(screen.getByRole('button', { name: /send event/i }));

    // App throws new Error(body) then catches it, calling setResult(e.message)
    expect(await screen.findByText(/internal server error/i)).toBeInTheDocument();
  });
});
