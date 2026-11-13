import { afterEach, expect, it, vi } from 'vitest';
import { VoiceSession } from '../desktop/renderer/voice/session';
import type { VoiceEvent } from '../desktop/shared/voice';

vi.mock('../desktop/renderer/voice/microphone-capture.js', () => ({
  createMicrophoneCaptureLifecycle: () => ({ start() {}, stop() {} }),
}));
vi.mock('../desktop/renderer/voice/playback-lifecycle.js', () => ({
  createPlaybackOutputFollow: () => ({ start() {}, stop() {} }),
}));
afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

async function fixture(sampleRate = 24000) {
  vi.useFakeTimers();
  const sources: any[] = [];
  const context = { currentTime: 0, sampleRate, destination: {}, resume: vi.fn(), close: vi.fn().mockResolvedValue(undefined),
    createBuffer: (_channels: number, length: number, rate: number) => ({ duration: length / rate, sampleRate: rate,
      samples: new Float32Array(length), copyToChannel(samples: Float32Array) { this.samples.set(samples); } }),
    createBufferSource: () => {
      const source = { buffer: undefined, connect() {}, disconnect() {}, start: vi.fn(), stop: vi.fn(), onended: () => {} };
      sources.push(source); return source;
    },
  };
  vi.stubGlobal('AudioContext', function () { return context; });
  vi.stubGlobal('navigator', { mediaDevices: {} });
  let receive!: (event: VoiceEvent) => void;
  const api = { start: vi.fn().mockResolvedValue(undefined), stop: vi.fn().mockResolvedValue(undefined),
    send: vi.fn().mockResolvedValue(true), onEvent: (callback: typeof receive) => { receive = callback; return () => {}; } };
  const event = vi.fn(), session = new VoiceSession(api, event);
  await session.start();
  const audio = Buffer.alloc(24000 * 2).toString('base64');
  const chunk = (id = 'result', data = audio) => receive({ type: 'audio.delta', responseId: id, sampleRate: 24000, audio: data });
  return { session, api, event, context, sources, receive, chunk };
}

it('plays a fast burst of two minutes of speech completely and acknowledges it once', async () => {
  const f = await fixture();
  for (let i = 0; i < 120; i++) f.chunk();
  f.receive({ type: 'audio.done', responseId: 'result' });
  await vi.advanceTimersByTimeAsync(1000);
  expect(f.sources).toHaveLength(120);
  expect(f.api.send.mock.calls.map(([e]) => e.type)).toEqual(['playback.started']);
  for (const source of f.sources) { f.context.currentTime++; source.onended(); }
  expect(f.api.send.mock.calls.map(([e]) => e.type)).toEqual(['playback.started', 'playback.ended']);
  expect(f.sources.every(source => !source.stop.mock.calls.length)).toBe(true);
  await f.session.stop();
});

it('keeps speech continuous when 40 ms packets arrive with network jitter', async () => {
  const f = await fixture();
  const audio = Buffer.alloc(960 * 2, 0x25).toString('base64');
  for (let i = 0; i < 30; i++) {
    f.context.currentTime = i * 0.04 + (i % 5 === 2 ? 0.07 : i % 5 === 3 ? 0.04 : 0);
    f.chunk('jitter', audio);
  }
  f.receive({ type: 'audio.done', responseId: 'jitter' });
  for (let i = 1; i < f.sources.length; i++) {
    const previous = f.sources[i - 1];
    expect(f.sources[i].start.mock.calls[0][0]).toBeCloseTo(previous.start.mock.calls[0][0] + previous.buffer.duration, 9);
  }
  expect(f.sources.reduce((sum, source) => sum + source.buffer.duration, 0)).toBeCloseTo(1.2, 9);
  await f.session.stop();
});

it('prebuffers each response, flushes short speech, and clears audio that has not reached the speaker', async () => {
  const f = await fixture();
  const audio = Buffer.alloc(960 * 2, 0x25).toString('base64');
  f.chunk('cancelled', audio);
  expect(f.sources).toHaveLength(0);
  f.session.interrupt();
  expect(f.api.send).toHaveBeenCalledWith({ type: 'playback.cancelled', responseId: 'cancelled', reason: 'user_interruption' });
  f.chunk('cancelled', audio);
  f.receive({ type: 'audio.done', responseId: 'cancelled' });
  await vi.advanceTimersByTimeAsync(1000);
  expect(f.sources).toHaveLength(0);
  f.receive({ type: 'response.started', origin: 'model', responseId: 'short' });
  f.chunk('short', audio);
  expect(f.sources).toHaveLength(0);
  f.receive({ type: 'audio.done', responseId: 'short' });
  expect(f.sources).toHaveLength(1);
  const presentation = f.event.mock.calls.filter(([event]) => event.type === 'desktop.presentation').at(-1)![0].presentation;
  expect(presentation.messages[0].streaming).toBe(true);
  expect(f.api.send).not.toHaveBeenCalledWith({ type: 'playback.ended', responseId: 'short' });
  f.sources[0].onended();
  expect(f.api.send).toHaveBeenCalledWith({ type: 'playback.ended', responseId: 'short' });
  f.chunk('next', audio);
  expect(f.sources).toHaveLength(1);
  await f.session.stop();
  await vi.advanceTimersByTimeAsync(1000);
  expect(f.sources).toHaveLength(1);
});

it.each([44100, 48000])('resamples the entire response continuously to the %i Hz speaker clock', async rate => {
  const f = await fixture(rate);
  const pcm = Buffer.alloc(24003 * 2);
  for (let i = 0; i < pcm.length / 2; i++) pcm.writeInt16LE(Math.round(Math.sin(i * 0.017) * 16000), i * 2);
  for (let offset = 0; offset < pcm.length; offset += 1920) f.chunk('resampled', pcm.subarray(offset, offset + 1920).toString('base64'));
  f.receive({ type: 'audio.done', responseId: 'resampled' });
  expect(f.sources.every(source => source.buffer.sampleRate === rate)).toBe(true);
  const actual = f.sources.flatMap(source => Array.from(source.buffer.samples as Float32Array));
  expect(actual).toHaveLength(Math.round(pcm.length / 2 * rate / 24000));
  for (let i = 0; i < actual.length; i++) {
    const position = i * 24000 / rate, before = Math.floor(position), fraction = position - before;
    const after = Math.min(pcm.length / 2 - 1, before + 1);
    const expected = (pcm.readInt16LE(before * 2) * (1 - fraction) + pcm.readInt16LE(after * 2) * fraction) / 32768;
    if (Math.abs(actual[i] - expected) > 1e-6) throw new Error(`Discontinuity at sample ${i}: ${actual[i]} vs ${expected}`);
  }
  await f.session.stop();
});

it('interrupts queued long speech, ignores late chunks, and can play a new response', async () => {
  const f = await fixture();
  for (let i = 0; i < 90; i++) f.chunk();
  f.session.interrupt();
  expect(f.api.send).toHaveBeenCalledWith({ type: 'playback.cancelled', responseId: 'result', reason: 'user_interruption' });
  expect(f.sources.every(source => source.stop.mock.calls.length === 1)).toBe(true);
  f.chunk(); f.receive({ type: 'audio.done', responseId: 'result' });
  expect(f.sources).toHaveLength(90);
  for (const source of f.sources) source.onended();
  f.chunk('next'); f.receive({ type: 'audio.done', responseId: 'next' });
  f.sources.at(-1).onended();
  expect(f.api.send).toHaveBeenCalledWith({ type: 'playback.ended', responseId: 'next' });
  await f.session.stop();
});

it('bounds buffered PCM, reports an overflow once, and releases memory for subsequent speech', async () => {
  const f = await fixture();
  for (let i = 0; i < 750; i++) f.chunk();
  expect(f.api.send.mock.calls).toEqual([[{ type: 'playback.cancelled', responseId: 'result', reason: 'playback_overflow' }]]);
  expect(f.event).toHaveBeenCalledWith(expect.objectContaining({ type: 'error', message: expect.stringContaining('完整结果') }));
  for (const source of f.sources) source.onended();
  f.chunk('next'); f.receive({ type: 'audio.done', responseId: 'next' });
  f.sources.at(-1).onended();
  expect(f.api.send).toHaveBeenCalledWith({ type: 'playback.ended', responseId: 'next' });
  await f.session.stop();
});

it('forwards live text to the chat projection and releases the next bubble only on speaker completion', async () => {
  const f = await fixture();
  const presentation = () => f.event.mock.calls.map(([event]) => event).filter(event => event.type === 'desktop.presentation').at(-1).presentation;
  f.receive({ type: 'response.started', origin: 'model', responseId: 'config' });
  f.receive({ type: 'transcript.delta', role: 'assistant', responseId: 'config', content: '电脑配置的第一段' });
  expect(presentation().messages[0].text).toBe('电脑配');
  f.chunk('config');
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存实时读数' });
  f.receive({ type: 'audio.done', responseId: 'config' });
  expect(presentation().messages[1].visible).toBe(false);
  f.context.currentTime = 0.5;
  await vi.advanceTimersByTimeAsync(500);
  expect(presentation().messages[0].text.length).toBeGreaterThan(3);
  expect(presentation().messages[1].visible).toBe(false);
  f.context.currentTime = 1.1; f.sources[0].onended();
  expect(presentation().messages[1].visible).toBe(true);
  f.receive({ type: 'playback.clear', reason: 'user_interruption' });
  expect(presentation().messages[1].streaming).toBe(false);
  expect(f.api.send.mock.calls.some(([event]) => ['response.cancel', 'input_audio_mute.commit'].includes(event.type))).toBe(false);
  await f.session.stop();
});
