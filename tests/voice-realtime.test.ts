import { afterEach, expect, it, vi } from 'vitest';
import { RealtimeVoice, voiceToolCatalog, type VoiceWireHandlers } from '../desktop/shared/voice-realtime';
import type { VoiceEvent } from '../desktop/shared/voice';

function opening(seedHistory = false, dictation = false) {
  const connections: VoiceWireHandlers[] = [];
  const wire: VoiceEvent[] = [],
    events: VoiceEvent[] = [];
  const runTool = vi.fn(async () => ({ status: 'accepted' }));
  const socket = {
    bufferedAmount: 0,
    send(text: string) {
      wire.push(JSON.parse(text));
    },
    close: vi.fn(),
  };
  const voice = new RealtimeVoice({
    dictation,
    connect(handlers) {
      connections.push(handlers);
      return socket;
    },
    emit: event => events.push(event),
    tools: voiceToolCatalog,
    runTool,
    history: () => seedHistory ? [
      { role: 'assistant', content: 'orphan' },
      { role: 'user', content: 'question' },
      { role: 'assistant', content: 'answer' },
      { role: 'user', content: 'unanswered' },
    ] : [],
  });
  const receive = (event: VoiceEvent) => connections.at(-1)!.message(JSON.stringify(event));
  const joined = voice.start();
  return { voice, wire, events, receive, connections, runTool, socket, joined };
}

async function call(seedHistory = false, dictation = false) {
  const c = opening(seedHistory, dictation);
  c.connections[0].open();
  c.receive({ type: 'session.created' });
  await c.joined;
  return c;
}

afterEach(() => vi.useRealTimers());

const asr = 'conversation.item.input_audio_transcription.';

it.each([false, true])('recovers a transient first handshake without a second start (dictation: %s)', async dictation => {
  vi.useFakeTimers();
  const c = opening(false, dictation);
  const first = c.connections[0];
  if (dictation) first.error('连接被重置', true);
  else first.close(1006);
  expect(c.voice.start()).toBe(c.joined);
  await vi.advanceTimersByTimeAsync(500);
  expect(c.connections).toHaveLength(2);
  first.error('过期的连接', false);
  first.open();
  first.message('{"type":"session.created"}');
  expect(c.wire).toHaveLength(0);
  c.connections[1].open();
  c.receive({ type: 'session.created' });
  await c.joined;
  expect(c.events.filter(e => e.type === 'desktop.voice').map(e => e.state)).toEqual(['connecting', 'ready']);
  c.voice.stop();
});

it('only retries a transient first connection once', async () => {
  vi.useFakeTimers();
  const c = opening();
  const failure = expect(c.joined).rejects.toThrow('语音连接已断开');
  c.connections[0].close(1011);
  await vi.advanceTimersByTimeAsync(500);
  c.connections[1].close(1006);
  await failure;
  await vi.advanceTimersByTimeAsync(35_000);
  expect(c.connections).toHaveLength(2);
});

it('keeps the original 35 second deadline across a slow first handshake and retry', async () => {
  vi.useFakeTimers();
  const c = opening(false, true);
  const failure = expect(c.joined).rejects.toThrow('连接超时');
  await vi.advanceTimersByTimeAsync(30_000);
  c.connections[0].error('临时网络错误', true);
  await vi.advanceTimersByTimeAsync(4_999);
  expect(c.connections).toHaveLength(2);
  expect(c.events.some(e => e.state === 'error')).toBe(false);
  await vi.advanceTimersByTimeAsync(1);
  await failure;
});

it('cancels during the first reconnect delay', async () => {
  vi.useFakeTimers();
  const c = opening();
  c.connections[0].error('临时网络错误', true);
  const cancelled = expect(c.joined).rejects.toThrow('已结束');
  c.voice.stop();
  await cancelled;
  await vi.advanceTimersByTimeAsync(35_000);
  expect(c.connections).toHaveLength(1);
});

it('surfaces a rejected handshake and does not retry a rate-limited call', async () => {
  vi.useFakeTimers();
  const pending = opening();
  const failure = expect(pending.joined).rejects.toThrow('凭据失效');
  pending.connections[0].error('凭据失效', false);
  await failure;
  const connected = await call();
  connected.receive({ type: 'error', error: { code: 'rate_limit_exceeded', message: '请求过于频繁' } });
  await vi.advanceTimersByTimeAsync(35_000);
  expect(pending.connections).toHaveLength(1);
  expect(connected.connections).toHaveLength(1);
  expect(connected.events.at(-1)).toMatchObject({ state: 'error', message: '请求过于频繁' });
});

it('returns dictated words once, including fillers, without chat, speech or tools', async () => {
  const c = await call(true, true);
  try {
    expect(c.wire[0]).toMatchObject({ type: 'session.create', session: { tools: [] } });
    expect(c.wire.some(e => e.type === 'conversation.item.create')).toBe(false);
    c.voice.speak('通知');
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=' });
    c.receive({ type: asr + 'delta', item_id: 'a', delta: '请记' });
    c.receive({ type: asr + 'text', item_id: 'a', text: '请记下', stash: '这件事。' });
    c.receive({ type: asr + 'completed', item_id: 'a' });
    c.receive({ type: asr + 'completed', item_id: 'a', text: '重复的完成事件' });
    c.receive({ type: 'response.output_text.done', text: '这是回答' });
    c.receive({ type: 'response.output_audio.delta', audio: 'invalid audio' });
    c.receive({ type: 'response.function_call_arguments.done', items: [
      { call_id: 'reject', name: 'spawn_thinking', arguments: '{"objective":"发起任务"}' },
    ] });
    c.receive({ type: 'response.done' });
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=', localActivity: { started: true, speaking: true } });
    const result = c.voice.finishDictation();
    let finished = false;
    void result.then(() => { finished = true; });
    expect(c.voice.finishDictation()).toBe(result);
    expect(c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=' })).toBe(false);
    c.receive({ type: 'input_audio_buffer.committed' });
    await Promise.resolve();
    expect(finished).toBe(false);
    c.receive({ type: asr + 'completed', item_id: 'b', text: '嗯' });
    await expect(result).resolves.toBe('请记下这件事。\n嗯');
    expect(c.wire.map(e => e.type)).toEqual(['session.create', 'input_audio_unmute.commit',
      'input_audio_buffer.append', 'input_audio_buffer.append', 'input_audio_buffer.commit']);
    expect(c.events.map(e => e.type)).toEqual(['desktop.voice', 'desktop.voice', 'voice.ready']);
    expect(c.runTool).not.toHaveBeenCalled();
  } finally { c.voice.stop(); }
});

it('waits for every dictated item and keeps the order in which speech started', async () => {
  const c = await call(false, true);
  try {
    c.receive({ type: 'input_audio_buffer.speech_started', item_id: 'first' });
    c.receive({ type: asr + 'started', item_id: 'second' });
    const result = c.voice.finishDictation();
    c.receive({ type: asr + 'completed', item_id: 'second', text: '第二句' });
    c.receive({ type: asr + 'completed', item_id: 'first', text: '第一句' });
    await expect(result).resolves.toBe('第一句\n第二句');
  } finally { c.voice.stop(); }
});

it('keeps waiting when an earlier item finishes after the next speech onset', async () => {
  const c = await call(false, true);
  try {
    c.receive({ type: asr + 'delta', item_id: 'first', delta: '第一句' });
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=', localActivity: { started: true, speaking: true } });
    const result = c.voice.finishDictation();
    c.receive({ type: asr + 'delta', item_id: 'first', delta: '第一句' });
    c.receive({ type: asr + 'completed', item_id: 'first' });
    c.receive({ type: asr + 'completed', item_id: 'last', text: '最后一句' });
    await expect(result).resolves.toBe('第一句\n最后一句');
  } finally { c.voice.stop(); }
});

it('accepts continued text in the same item across a local speech pause', async () => {
  const c = await call(false, true);
  try {
    c.receive({ type: asr + 'delta', item_id: 'one', delta: '请记' });
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=', localActivity: { started: true, speaking: true } });
    const result = c.voice.finishDictation();
    c.receive({ type: asr + 'delta', item_id: 'one', delta: '请记一下明天的会议' });
    c.receive({ type: asr + 'completed', item_id: 'one' });
    await expect(result).resolves.toBe('请记一下明天的会议');
  } finally { c.voice.stop(); }
});

it('handles a provider speech onset arriving ahead of Silero', async () => {
  const c = await call(false, true);
  try {
    c.receive({ type: asr + 'started', item_id: 'early' });
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=', localActivity: { started: true, speaking: true } });
    const result = c.voice.finishDictation();
    c.receive({ type: asr + 'completed', item_id: 'early', text: '完整的一句。' });
    await expect(result).resolves.toBe('完整的一句。');
  } finally { c.voice.stop(); }
});

it('preserves confirmed dictation when local activity never produces another provider item', async () => {
  vi.useFakeTimers();
  const c = await call(false, true);
  try {
    c.receive({ type: asr + 'completed', item_id: 'confirmed', text: '保留这句话。' });
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=', localActivity: { started: true, speaking: true } });
    const result = c.voice.finishDictation();
    await vi.advanceTimersByTimeAsync(8_000);
    await expect(result).resolves.toBe('保留这句话。');
  } finally { c.voice.stop(); }
});

it('returns already finalized dictation after silence and reports an empty result', async () => {
  const c = await call(false, true);
  try {
    c.receive({ type: asr + 'completed', item_id: 'a', text: '已经说完。' });
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=', localActivity: { started: false, speaking: false } });
    await expect(c.voice.finishDictation()).resolves.toBe('已经说完。');
  } finally { c.voice.stop(); }
  const empty = await call(false, true);
  try {
    const result = empty.voice.finishDictation();
    empty.receive({ type: asr + 'completed', item_id: 'empty', text: '' });
    await expect(result).rejects.toThrow('没有识别到文字');
  } finally { empty.voice.stop(); }
});

it('rejects dictation on timeout, a transcription error, or cancellation', async () => {
  vi.useFakeTimers();
  const c = await call(false, true);
  try {
    c.receive({ type: asr + 'delta', item_id: 'partial', delta: '未完成' });
    const timedOut = expect(c.voice.finishDictation()).rejects.toThrow('超时');
    await vi.advanceTimersByTimeAsync(8_000);
    await timedOut;
  } finally { c.voice.stop(); }
  const failed = await call(false, true);
  const failure = failed.voice.finishDictation();
  failed.receive({ type: asr + 'failed', item_id: 'failed' });
  await expect(failure).rejects.toThrow('语音转写失败');
  const cancelled = await call(false, true);
  const cancellation = cancelled.voice.finishDictation();
  cancelled.voice.stop();
  await expect(cancellation).rejects.toThrow('取消');
});

it('discards the disconnected dictation and ignores its late results on the next recording', async () => {
  vi.useFakeTimers();
  const c = await call(false, true);
  try {
    const stale = c.connections[0];
    c.receive({ type: asr + 'delta', item_id: 'old', delta: '旧内容' });
    const result = c.voice.finishDictation();
    stale.close(1006);
    await expect(result).rejects.toThrow('断开');
    await vi.advanceTimersByTimeAsync(60_000);
    expect(c.connections).toHaveLength(1);
    const restarted = c.voice.start();
    c.connections[1].open();
    c.receive({ type: 'session.created' });
    await restarted;
    const next = c.voice.finishDictation();
    stale.message(JSON.stringify({ type: asr + 'completed', item_id: 'old', text: '旧内容' }));
    c.receive({ type: asr + 'completed', item_id: 'new', text: '新内容' });
    await expect(next).resolves.toBe('新内容');
  } finally { c.voice.stop(); }
});

it.each(['换个话题', '停下'])('interrupts the old audio on recognized speech and lets the model handle "%s"', async text => {
  const c = await call();
  try {
    c.voice.speak('正在朗读任务结果', ['task']);
    const audio = { type: 'response.output_audio.delta', audio: 'AAAAAA==' };
    c.receive({ ...audio, response_id: 'reading' });
    c.receive({ type: 'conversation.item.input_audio_transcription.delta', item_id: 'new', delta: text });
    expect(c.events.filter(e => e.type === 'playback.cancelled')).toMatchObject([{ taskIds: ['task'] }]);
    c.receive({ ...audio, response_id: 'reading' });
    c.receive({ type: 'conversation.item.input_audio_transcription.completed', item_id: 'new', text });
    c.receive({ ...audio, response_id: 'reading' });
    c.receive({ ...audio, response_id: 'new-answer' });
    expect(c.events.filter(e => e.type === 'audio.delta').map(e => e.responseId)).toEqual(['reading', 'new-answer']);
    expect(c.events).toContainEqual(expect.objectContaining({ type: 'transcript.final', role: 'user', content: text }));
    expect(c.wire.filter(e => e.type === 'response.cancel')).toHaveLength(0);
  } finally { c.voice.stop(); }
});

it('interrupts a submitted announcement even before the first provider packet arrives', async () => {
  const c = await call();
  try {
    c.voice.speak('之前的任务结果', ['task']);
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=',
      localActivity: { started: true, speaking: true, barge: true, quietFrames: 0 } });
    expect(c.events.filter(e => e.type === 'playback.cancelled')).toMatchObject([{ taskIds: ['task'] }]);
    c.receive({ type: 'response.output_audio.started', response_id: 'cancelled-tts' });
    c.receive({ type: 'conversation.item.input_audio_transcription.completed', item_id: 'next', text: '另外一件事' });
    c.receive({ type: 'response.output_audio.delta', response_id: 'cancelled-tts', audio: 'AAAAAA==' });
    c.receive({ type: 'response.output_audio.delta', response_id: 'next', audio: 'AAAAAA==' });
    expect(c.events.filter(e => e.type === 'audio.delta')).toMatchObject([{ origin: 'model', responseId: 'next' }]);
  } finally { c.voice.stop(); }
});

it('keeps the call available when the model leaves a turn silent', async () => {
  vi.useFakeTimers();
  const c = await call();
  try {
    c.receive({ type: 'conversation.item.input_audio_transcription.completed', item_id: 'quiet', text: '我想一想' });
    await vi.advanceTimersByTimeAsync(65_000);
    expect(c.connections).toHaveLength(1);
    expect(c.events.some(e => e.type === 'error' || e.type === 'audio.delta')).toBe(false);
    c.receive({ type: 'conversation.item.input_audio_transcription.completed', item_id: 'next', text: '继续聊吧' });
    c.receive({ type: 'response.output_audio.delta', response_id: 'next', audio: 'AAAAAA==' });
    expect(c.events.at(-1)).toMatchObject({ type: 'audio.delta', responseId: 'next' });
  } finally { c.voice.stop(); }
});

it('establishes a voice session with paired history and only sends provider protocol fields for audio', async () => {
  const c = await call(true);
  try {
    expect(c.wire[0]).toMatchObject({
      type: 'session.create',
      session: { model: '1.2.6.1', audio: { input: { format: { rate: 16000 } } } },
    });
    expect((c.wire[1].items as { role: string }[]).map(row => row.role)).toEqual(['user', 'assistant']);
    c.socket.bufferedAmount = 65537;
    expect(c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=' })).toBe(false);
    c.socket.bufferedAmount = 0;
    c.voice.send({
      type: 'input_audio_buffer.append',
      audio: 'AAA=',
      localActivity: { started: true, speaking: true },
    });
    expect(c.wire.at(-1)).toEqual({
      type: 'input_audio_buffer.append',
      audio: 'AAA=',
      event_id: expect.any(String),
    });
    c.voice.send({ type: 'input.mute' });
    expect(c.wire.at(-1)?.type).toBe('input_audio_mute.commit');
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAAA' });
    expect(c.wire.at(-1)?.type).toBe('input_audio_mute.commit');
    c.voice.send({ type: 'input.unmute' });
    expect(c.wire.at(-1)?.type).toBe('input_audio_unmute.commit');
  } finally {
    c.voice.stop();
  }
});

it('handles PCM conversion, held local speech, one interruption, late audio, and terminal usage without replay', async () => {
  const c = await call();
  try {
    const delta = { type: 'response.output_audio.delta', response_id: 'first', delta: 'AACAPwAAgL8=' };
    c.receive(delta);
    expect(c.events.at(-1)).toMatchObject({ type: 'audio.delta', responseId: 'first', audio: '/38AgA==' });
    const mic = {
      type: 'input_audio_buffer.append',
      audio: 'AAA=',
      localActivity: { started: true, speaking: true, barge: false, quietFrames: 0 },
    };
    c.voice.send(mic);
    expect(c.wire.filter(e => e.type === 'response.cancel')).toHaveLength(0);
    mic.localActivity.barge = true;
    c.voice.send(mic);
    c.voice.send(mic);
    expect(c.wire.filter(e => e.type === 'response.cancel')).toHaveLength(1);
    c.receive(delta);
    c.receive({ type: 'response.done', response: { usage: { total_tokens: 10 } } });
    expect(c.events.filter(e => e.type === 'audio.delta')).toHaveLength(1);
    c.receive({
      type: 'conversation.item.input_audio_transcription.completed',
      item_id: 'a',
      text: '说完了',
    });
    c.receive({ ...delta, response_id: 'second' });
    c.receive({ type: 'response.output_audio.done', response_id: 'second' });
    c.receive({ ...delta, response_id: 'second' });
    expect(c.events.filter(e => e.type === 'audio.delta')).toHaveLength(2);
  } finally {
    c.voice.stop();
  }
});

it('requires a confirmed transcript before dispatching tasks, uses model arguments, deduplicates calls and batches results', async () => {
  const c = await call();
  try {
    const argumentsEvent = (id: string, ids: string[]) => ({
      type: 'response.function_call_arguments.done',
      response_id: id,
      items: ids.map(call_id => ({ call_id, name: 'spawn_thinking', arguments: '{"objective":"任务"}' })),
    });
    c.receive(argumentsEvent('r0', ['unconfirmed']));
    await vi.waitFor(() => expect(c.wire.at(-1)?.type).toBe('conversation.item.create'));
    expect(c.runTool).not.toHaveBeenCalled();
    c.receive({ type: 'response.output_audio.done', response_id: 'r0' });
    c.receive({ type: 'conversation.item.input_audio_transcription.delta', item_id: 'a', delta: '先' });
    c.receive({
      type: 'conversation.item.input_audio_transcription.completed',
      item_id: 'a',
      text: '现在帮我做',
    });
    const event = argumentsEvent('r1', ['once', 'twice', 'twice']);
    c.receive(event);
    c.receive(event);
    await vi.waitFor(() => expect(c.wire.at(-1)?.items).toHaveLength(2));
    expect(c.runTool).toHaveBeenCalledTimes(2);
    expect(c.runTool).toHaveBeenCalledWith(
      expect.objectContaining({ id: 'once', args: { objective: '任务' }, transcript: '现在帮我做' }),
    );
    const audio = { type: 'response.output_audio.delta', response_id: 'r1', audio: 'AAAAAA==' };
    c.receive(audio);
    c.receive({ type: 'response.output_audio.done', response_id: 'r1' });
    c.receive({ ...audio, response_id: 'repeat-after-tool-reply' });
    expect(c.events.filter(event => event.type === 'audio.delta').map(event => event.responseId)).toEqual(['r1']);
  } finally {
    c.voice.stop();
  }
});

it('binds an announcement to its playback receipt and queues further speech until playback has drained', async () => {
  const c = await call();
  try {
    c.voice.speak('一', ['task1']);
    c.voice.speak('二', ['task2']);
    c.receive({ type: 'response.output_audio.delta', response_id: 'a', delta: 'AAAAAA==' });
    c.receive({ type: 'response.output_audio.done', response_id: 'a' });
    expect(c.wire.filter(e => e.type === 'speech_text_buffer.commit').map(e => e.text)).toEqual(['一']);
    expect(c.events.find(e => e.type === 'transcript.final')).toMatchObject({
      origin: 'announcement',
      taskIds: ['task1'],
      content: '一',
    });
    c.voice.send({ type: 'playback.ended', responseId: 'a' });
    expect(c.events.filter(e => e.type.startsWith('playback.'))).toMatchObject([
      { type: 'playback.requested', taskIds: ['task1'] },
      { type: 'playback.ended', taskIds: ['task1'] },
      { type: 'playback.requested', taskIds: ['task2'] },
    ]);
    expect(c.wire.at(-1)).toMatchObject({ type: 'speech_text_buffer.commit', text: '二' });
  } finally {
    c.voice.stop();
  }
});

it('recovers a call without replaying old wire events, preserves microphone order and releases reconnect timers on stop', async () => {
  vi.useFakeTimers();
  const c = await call();
  try {
    c.connections[0].close();
    const reconnect = c.voice.start();
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=' });
    await vi.advanceTimersByTimeAsync(500);
    c.connections[1].open();
    c.receive({ type: 'session.created' });
    await vi.advanceTimersByTimeAsync(0);
    await reconnect;
    expect(c.wire.at(-1)).toMatchObject({ type: 'input_audio_buffer.append', audio: 'AAA=' });
    c.connections[0].message(
      JSON.stringify({ type: 'response.output_audio.delta', response_id: 'stale', audio: 'AAAAAA==' }),
    );
    expect(c.events.some(e => e.type === 'audio.delta')).toBe(false);
    c.connections[1].close();
    c.voice.stop();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(c.connections).toHaveLength(2);
  } finally {
    c.voice.stop();
  }
});

it('labels typed speech and returns a single playback cancellation on interruption', async () => {
  const c = await call();
  try {
    c.voice.speak('文字消息', [], 'text');
    c.receive({ type: 'response.output_audio.delta', response_id: 'typed', delta: 'AAAAAA==' });
    expect(c.events.at(-1)).toMatchObject({ type: 'audio.delta', origin: 'text' });
    c.voice.send({ type: 'response.cancel' });
    c.voice.send({ type: 'playback.cancelled', responseId: 'typed' });
    expect(c.events.filter(e => e.type === 'playback.cancelled')).toMatchObject([
      { origin: 'text', responseId: 'typed' },
    ]);
  } finally {
    c.voice.stop();
  }
});

it('commits untranscribed speech after silence then reconnects if ASR is still missing', async () => {
  vi.useFakeTimers();
  const c = await call();
  try {
    c.voice.send({
      type: 'input_audio_buffer.append',
      audio: 'AAA=',
      localActivity: { started: true, speaking: true, barge: false, quietFrames: 0 },
    });
    await vi.advanceTimersByTimeAsync(5_000);
    expect(c.wire.at(-1)?.type).toBe('input_audio_buffer.commit');
    await vi.advanceTimersByTimeAsync(2_500);
    expect(c.connections).toHaveLength(2);
    expect(c.events).toContainEqual(expect.objectContaining({ type: 'desktop.voice', state: 'connecting' }));
  } finally {
    c.voice.stop();
  }
});

it('does not start queued TTS on a renderer cancellation, even if the provider has finished sending audio', async () => {
  const c = await call();
  try {
    c.voice.speak('正在读', ['first']);
    c.voice.speak('还没读', ['queued']);
    c.receive({ type: 'response.output_audio.delta', response_id: 'r', audio: 'AAAAAA==' });
    c.receive({ type: 'response.output_audio.done', response_id: 'r' });
    c.voice.send({ type: 'playback.cancelled', responseId: 'r', reason: 'user_interruption' });
    c.voice.send({ type: 'response.cancel' });
    c.receive({ type: 'response.created' });
    c.receive({ type: 'response.output_audio.delta', audio: 'AAAAAA==' });
    c.receive({ type: 'response.output_audio.delta', response_id: 'late', audio: 'AAAAAA==' });
    expect(c.wire.filter(e => e.type === 'speech_text_buffer.commit').map(e => e.text)).toEqual(['正在读']);
    expect(c.events.filter(e => e.type === 'audio.delta')).toHaveLength(1);
    expect(c.events.filter(e => e.type === 'playback.cancelled').map(e => e.taskIds)).toEqual([['first'], ['queued']]);
    c.receive({ type: 'conversation.item.input_audio_transcription.completed', item_id: 'fresh', text: '新问题' });
    c.receive({ type: 'response.output_audio.delta', response_id: 'late', audio: 'AAAAAA==' });
    c.receive({ type: 'response.output_audio.delta', response_id: 'fresh', audio: 'AAAAAA==' });
    expect(c.events.filter(e => e.type === 'audio.delta').map(e => e.responseId)).toEqual(['r', 'fresh']);
  } finally { c.voice.stop(); }
});

it('retires submitted and queued announcements on hangup before the provider supplies a response ID', async () => {
  const c = await call();
  const send = c.socket.send.bind(c.socket);
  let recordedBeforeSend = false;
  c.socket.send = text => {
    if (JSON.parse(text).type === 'speech_text_buffer.commit')
      recordedBeforeSend = c.events.at(-1)?.type === 'playback.requested';
    send(text);
  };
  c.voice.speak('已提交', ['sent']);
  c.voice.speak('排队', ['queued']);
  c.voice.stop();
  expect(recordedBeforeSend).toBe(true);
  expect(c.events.filter(e => e.type === 'playback.requested').map(e => e.taskIds)).toEqual([['sent']]);
  expect(c.events.filter(e => e.type === 'playback.cancelled')).toMatchObject([
    { reason: 'call_ended', taskIds: ['sent'] }, { reason: 'call_ended', taskIds: ['queued'] },
  ]);
});

it('does not voice restored history on first join or reconnect and leaves unsubmitted task notifications available', async () => {
  vi.useFakeTimers();
  const c = await call(true);
  try {
    const audio = { type: 'response.output_audio.delta', audio: 'AAAAAA==' };
    c.receive({ ...audio, response_id: 'history' });
    c.voice.speak('读到一半', ['submitted']);
    c.receive({ ...audio, response_id: 'old' });
    c.voice.speak('下一个任务', ['later']);
    c.connections[0].close(1006);
    expect(c.events.filter(e => e.type === 'playback.cancelled')).toMatchObject([
      { reason: 'disconnected', taskIds: ['submitted'] },
    ]);
    await vi.advanceTimersByTimeAsync(500);
    c.connections[1].open();
    c.receive({ type: 'session.created' });
    await vi.advanceTimersByTimeAsync(0);
    c.receive({ type: 'response.created' });
    c.receive(audio);
    c.receive({ ...audio, response_id: 'history-on-new-socket' });
    c.receive({ type: 'conversation.item.input_audio_transcription.completed', item_id: 'noise', text: '嗯' });
    c.receive(audio);
    expect(c.events.filter(e => e.type === 'audio.delta').map(e => e.responseId)).toEqual(['old']);
    c.voice.speak('下一个任务', ['later']);
    c.receive({ ...audio, response_id: 'old' });
    c.receive({ ...audio, response_id: 'later' });
    expect(c.events.filter(e => e.type === 'audio.delta').map(e => e.responseId)).toEqual(['old', 'later']);
  } finally { c.voice.stop(); }
});

it('drops an unidentified audio tail after a new question is transcribed, then plays the new stream', async () => {
  const c = await call();
  try {
    c.receive({ type: 'response.output_audio.delta', response_id: 'first', audio: 'AACAPw==' });
    c.voice.send({ type: 'input_audio_buffer.append', audio: 'AAA=',
      localActivity: { started: true, speaking: true, barge: true, quietFrames: 0 } });
    c.receive({ type: 'conversation.item.input_audio_transcription.completed', item_id: 'second', text: '明天几点出发？' });
    const from = c.events.length;
    // Seeduplex audio chunks can omit response_id. A tail chunk alone cannot announce another response.
    c.receive({ type: 'response.output_text.done', text: '上一段的结尾' });
    c.receive({ type: 'response.output_audio.delta', audio: 'AACAPw==' });
    c.receive({ type: 'response.output_audio.done' });
    c.receive({ type: 'response.output_audio.started' });
    c.receive({ type: 'response.output_audio.delta', audio: 'AACAvw==' });
    expect(c.events.slice(from).filter(e => e.type === 'audio.delta').map(e => e.audio)).toEqual(['AIA=']);
    expect(c.events.slice(from).filter(e => e.type === 'response.started')).toHaveLength(1);
    expect(c.events.slice(from).some(e => e.content === '上一段的结尾')).toBe(false);
  } finally { c.voice.stop(); }
});
