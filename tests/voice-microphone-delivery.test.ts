import { afterEach, expect, it, vi } from 'vitest';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { VoiceSession } from '../desktop/renderer/voice/session';
import { useVoice } from '../desktop/renderer/voice/use-voice';
import type { VoiceAction, VoiceEvent } from '../desktop/shared/voice';

afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

function microphone(send: (event: VoiceEvent) => Promise<boolean>) {
  vi.useFakeTimers();
  const port = new EventTarget();
  vi.stubGlobal('AudioWorkletNode', class { port = port; connect() {} disconnect() {} });
  vi.stubGlobal('AudioContext', class {
    currentTime = 0; sampleRate = 16000; destination = {};
    audioWorklet = { addModule: async () => {} };
    createMediaStreamSource() { return { connect() {}, disconnect() {} }; }
    async resume() {} async close() {}
  });
  const track = Object.assign(new EventTarget(), { stop() {} });
  const media = { getTracks: () => [track], getAudioTracks: () => [track] };
  vi.stubGlobal('navigator', { mediaDevices: Object.assign(new EventTarget(), { getUserMedia: async () => media }) });
  let listener: ((event: VoiceEvent) => void) | undefined;
  const api = { send, start: vi.fn(async () => { listener?.({ type: 'desktop.voice', state: 'ready' }); }),
    stop: vi.fn(async () => { listener?.({ type: 'desktop.voice', state: 'stopped' }); }),
    onEvent: (callback: (event: VoiceEvent) => void) => { listener = callback; return () => { listener = undefined; }; } };
  const frame = (value: number) => port.dispatchEvent(new MessageEvent('message', {
    data: { type: 'samples', samples: new Float32Array(128).fill(value).buffer },
  }));
  return { api, frame, receive: (event: VoiceEvent) => listener?.(event) };
}

async function capture(send: (event: VoiceEvent) => Promise<boolean>) {
  const mic = microphone(send), onEvent = vi.fn();
  const session = new VoiceSession(mic.api, onEvent);
  await session.start();
  await vi.advanceTimersByTimeAsync(0);
  return { ...mic, session, onEvent };
}

it.each([[0, false], [60, false], [120, false], [120, true]] as const)(
  'preserves microphone PCM with a %i ms IPC delay (every packet: %s)', async (delay, everyPacket) => {
  const delivered: Buffer[] = [];
  const mic = await capture(async event => {
    if (event.type === 'input_audio_buffer.append') {
      if (delay && (!delivered.length || everyPacket)) await new Promise(resolve => setTimeout(resolve, delay));
      delivered.push(Buffer.from(String(event.audio), 'base64'));
    }
    return true;
  });
  const expected = Buffer.alloc(50 * 128 * 2);
  for (let i = 0; i < 50; i++) {
    const amplitude = (i + 1) / 64;
    mic.frame(amplitude);
    for (let j = 0; j < 128; j++) expected.writeInt16LE(Math.trunc(amplitude * 0x7fff), (i * 128 + j) * 2);
    await vi.advanceTimersByTimeAsync(8);
  }
  // The final backlog can need one reply to release it and another to acknowledge it.
  await vi.advanceTimersByTimeAsync(2 * delay + 40);
  await mic.session.stop();
  const pcm = Buffer.concat(delivered);
  expect(pcm.byteLength / 2).toBe(expected.byteLength / 2);
  expect(pcm.equals(expected)).toBe(true);
});

it('retries a declined packet together with later PCM, preserving each sample once', async () => {
  const accepted: number[] = [];
  let attempts = 0;
  const mic = await capture(async event => {
    if (event.type !== 'input_audio_buffer.append') return true;
    if (++attempts === 1) return false;
    const pcm = Buffer.from(String(event.audio), 'base64');
    for (let offset = 0; offset < pcm.length; offset += 2) accepted.push(pcm.readInt16LE(offset));
    return true;
  });
  for (let i = 0; i < 10; i++) { mic.frame(i < 5 ? 0.25 : 0.5); await vi.advanceTimersByTimeAsync(8); }
  await vi.advanceTimersByTimeAsync(40);
  expect(accepted).toEqual([...Array(640).fill(8191), ...Array(640).fill(16383)]);
  await mic.session.stop();
});

it.each(['mute', 'disconnect'])('discards queued audio across %s including a pending refusal', async boundary => {
  let decline!: (value: boolean) => void;
  const attempts: number[] = [];
  const mic = await capture(async event => {
    if (event.type !== 'input_audio_buffer.append') return true;
    attempts.push(Buffer.from(String(event.audio), 'base64').readInt16LE());
    if (attempts.length === 1) return new Promise<boolean>(resolve => { decline = resolve; });
    return true;
  });
  for (let i = 0; i < 10; i++) mic.frame(0.25);
  if (boundary === 'mute') { mic.session.mute(true); mic.session.mute(false); }
  else mic.receive({ type: 'desktop.voice', state: 'stopped' });
  for (let i = 0; i < 5; i++) mic.frame(0.5);
  decline(false);
  await vi.advanceTimersByTimeAsync(120);
  expect(attempts).toEqual(boundary === 'mute' ? [8191, 16383] : [8191]);
  await mic.session.stop();
});

it('ends capture on sustained backpressure instead of sending a stale or clipped utterance', async () => {
  const mic = await capture(async () => false);
  for (let i = 0; i < 135; i++) { mic.frame(0.5); await vi.advanceTimersByTimeAsync(8); }
  const errors = mic.onEvent.mock.calls.map(([e]) => e).filter(e => e.type === 'desktop.voice' && e.state === 'error');
  expect(errors).toHaveLength(1);
  expect(errors[0].message).toContain('发送积压');
  expect(mic.api.stop).toHaveBeenCalledTimes(1);
});

it('reports an IPC failure without repeating a packet whose acceptance is unknown', async () => {
  const send = vi.fn(async () => { throw new Error('IPC unavailable'); });
  const mic = await capture(send);
  for (let i = 0; i < 15; i++) { mic.frame(0.5); await vi.advanceTimersByTimeAsync(8); }
  expect(send).toHaveBeenCalledTimes(1);
  expect(mic.onEvent).toHaveBeenCalledWith(expect.objectContaining({ type: 'desktop.voice', state: 'error', message: expect.stringContaining('发送失败') }));
  expect(mic.api.stop).toHaveBeenCalledTimes(1);
});

it('allows the voice control to start a new call immediately after microphone delivery fails', async () => {
  const send = vi.fn(async () => true).mockRejectedValueOnce(new Error('IPC unavailable'));
  const mic = microphone(send);
  let command!: (action: VoiceAction) => void;
  function VoiceControls() {
    command = useVoice(mic.api, 'fixture', true).command;
    return null;
  }
  renderToStaticMarkup(createElement(VoiceControls));
  for (let call = 0; call < 2; call++) {
    command('start');
    await vi.advanceTimersByTimeAsync(0);
    for (let i = 0; i < 5; i++) mic.frame(0.25);
    await vi.advanceTimersByTimeAsync(0);
  }
  expect(mic.api.start).toHaveBeenCalledTimes(2);
  expect(mic.api.stop).toHaveBeenCalledTimes(1);
  expect(send).toHaveBeenCalledTimes(2);
  command('stop');
  expect(mic.api.stop).toHaveBeenCalledTimes(2);
});
