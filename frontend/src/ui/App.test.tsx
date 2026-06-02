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
