import { expect, it } from 'vitest';
import { VoicePresentationStream } from '../desktop/renderer/voice/presentation';
import { voiceMessages } from '../desktop/renderer/chat/models/voice-messages';
import { normalizeHistoryRows } from '../desktop/renderer/chat/services/history-cache';
import { isVoiceState, type VoicePresentation, type VoiceState } from '../desktop/shared/voice';
import type { Message } from '../desktop/renderer/chat/models/chat';

function fixture() {
  let value!: VoicePresentation;
  const stream = new VoicePresentationStream('scene', snapshot => { value = snapshot; });
  const receive = (event: Parameters<typeof stream.receive>[0]) => { stream.receive(event); stream.tick(0); };
  const state = (): VoiceState => ({ state: 'ready', muted: false, error: '', transcript: '', task: '', presentation: value });
  const message = (key: string, text: string): Message => ({ kind: 'message', id: key, role: 'being',
    text, streaming: false, timestamp: '', label: 'Being', consecutive: false, sceneId: 'scene',
    createdAt: Date.now(), voiceKey: key, voicePending: true });
  return { stream, receive, state, message, value: () => value };
}

it('renders actual voice deltas before transcript.final or audio.done and advances with the speaker clock', () => {
  const f = fixture();
  f.receive({ type: 'response.started', responseId: 'config', origin: 'model' });
  f.receive({ type: 'transcript.delta', role: 'assistant', responseId: 'config', content: '这台电脑的配置是' });
  expect(f.value().messages[0].text).toBe('这台电');
  expect(f.value().messages[0].streaming).toBe(true);
  f.stream.audio('config', 0, 10);
  f.stream.tick(0.5);
  expect(f.value().messages[0].text.length).toBeGreaterThan(3);
  f.receive({ type: 'transcript.delta', role: 'assistant', responseId: 'config', content: '实时读取的结果，继续说明。' });
  f.stream.tick(1);
  expect(f.value().messages[0].text).toBe('这台电脑的配置是实时读');
  f.receive({ type: 'transcript.final', role: 'assistant', responseId: 'config', content: '这台电脑的配置是实时读取的结果，继续说明。' });
  f.receive({ type: 'audio.done', responseId: 'config' });
  const before = f.value().messages[0].text;
  f.stream.tick(1);
  expect(f.value().messages[0].text).toBe(before);
  expect(f.value().messages[0].text.length).toBeLessThan(22);
  f.stream.ended('config'); f.stream.tick(10);
  expect(f.value().messages[0].streaming).toBe(false);
});

it('keeps the next result hidden through history refresh and audio.done until actual playback ends', () => {
  const f = fixture();
  f.receive({ type: 'response.started', responseId: 'config', origin: 'announcement' });
  f.receive({ type: 'transcript.final', role: 'assistant', origin: 'announcement', responseId: 'config', taskIds: ['config'], content: '电脑配置详细结果' });
  f.stream.audio('config', 0, 20);
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存实时读数' });
  f.receive({ type: 'audio.done', responseId: 'config' });
  const saved = [f.message('agent:config', '电脑配置详细结果'), f.message('agent:memory', '内存实时读数')];
  expect(voiceMessages(saved, f.state()).map(row => row.id)).toEqual(['voice-live:agent:config']);
  f.stream.tick(5);
  expect(voiceMessages(saved, f.state())).toHaveLength(1);
  f.stream.ended('config'); f.stream.tick(20);
  expect(voiceMessages(saved, f.state()).map(row => row.id)).toEqual(['voice-live:agent:config', 'voice-live:agent:memory']);
  expect((voiceMessages(saved, f.state())[1] as Message).text).toBe('内存实');
  expect(saved[0].text).toBe('电脑配置详细结果');
});

it('streams Being output immediately, replaces replay snapshots, and never creates a duplicate archive bubble', () => {
  const f = fixture();
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存' });
  expect(voiceMessages([], f.state()).map(row => 'text' in row && row.text)).toEqual(['内存']);
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存实时读数' });
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存实时读数' });
  const projected = voiceMessages([f.message('agent:memory', '内存实时读数，完整结果')], f.state());
  expect(projected).toHaveLength(1);
  expect((projected[0] as Message).text).toBe('内存实时读数');
  expect((projected[0] as Message).streaming).toBe(true);
});

it('releases buffered results as text on interruption and ignores voice visibility when disconnected', () => {
  const f = fixture();
  f.receive({ type: 'response.started', responseId: 'config', origin: 'model' });
  f.receive({ type: 'transcript.delta', role: 'assistant', responseId: 'config', content: '配置说明' });
  f.stream.audio('config', 0, 10);
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存实时读数' });
  const saved = [f.message('voice:config', '配置说明'), f.message('agent:memory', '内存实时读数')];
  f.stream.clear(); f.stream.tick(1);
  expect(voiceMessages(saved, f.state()).map(row => (row as Message).text)).toEqual(['配置说明', '内存实时读数']);
  expect(voiceMessages(saved, { ...f.state(), state: 'stopped' }).filter(row => row.kind === 'message').map(row => row.text))
    .toEqual(['配置说明', '内存实时读数']);
  expect(isVoiceState(f.state())).toBe(true);
});

it('preserves presentation identity through normalization and hides a pending row even before its websocket event', () => {
  const f = fixture(); f.stream.tick(0);
  const rows = normalizeHistoryRows([{ seq: 1, role: 'assistant', content: '内存读数', voiceKey: 'agent:memory', voicePending: true }]);
  expect(rows[0].voiceKey).toBe('agent:memory');
  expect(rows[0].voicePending).toBe(true);
  expect(voiceMessages([f.message('agent:memory', '内存读数')], f.state())).toEqual([]);
  expect(voiceMessages([{ ...f.message('agent:memory', '内存读数'), sceneId: 'other' }], f.state())).toHaveLength(1);
});

it('keeps spoken progress separate from the task result and does not rewind a recovered Being stream', () => {
  const f = fixture();
  f.receive({ type: 'task.snapshot', tasks: [{ id: 'memory', notificationStatus: 'pending' }] });
  f.receive({ type: 'response.started', origin: 'progress', responseId: 'progress' });
  f.receive({ type: 'transcript.final', origin: 'progress', role: 'assistant', responseId: 'progress', taskIds: ['memory'], content: '正在读取内存' });
  f.stream.audio('progress', 0, 10);
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存读取完成' });
  expect(f.value().messages.find(row => row.key === 'agent:memory')?.visible).toBe(false);
  expect(f.value().messages.find(row => row.key === 'voice:progress')?.text).toBe('正在读');
  f.stream.ended('progress'); f.stream.tick(10);
  f.receive({ type: 'transcript.final', origin: 'announcement', role: 'assistant', responseId: 'result', taskIds: ['memory'], content: '内存读取完成' });
  f.stream.audio('result', 10, 1);
  f.receive({ type: 'audio.done', responseId: 'result' });
  f.stream.tick(11);
  const text = f.value().messages.find(row => row.key === 'agent:memory')?.text;
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存' });
  expect(f.value().messages.find(row => row.key === 'agent:memory')?.text).toBe(text);
});

it('never reopens an older result after an interruption and a newer spoken reply', () => {
  const f = fixture();
  const result = '内存实时读数：物理内存已用 88.5%，完整结果与建议。';
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存实时读数' });
  f.stream.clear(); f.stream.tick(0);
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: result });
  f.receive({ type: 'task.completed', task: { id: 'memory', status: 'completed', notificationStatus: 'pending', result } });
  f.receive({ type: 'response.started', responseId: 'disk', origin: 'model' });
  f.receive({ type: 'transcript.final', role: 'assistant', responseId: 'disk', origin: 'model', content: '好，我把磁盘分析也加上。' });
  f.stream.audio('disk', 0, 7);
  f.receive({ type: 'audio.done', responseId: 'disk' });
  f.stream.ended('disk'); f.stream.tick(7);
  const saved = [f.message('agent:memory', result), f.message('voice:disk', '好，我把磁盘分析也加上。')];
  expect((voiceMessages(saved, f.state())[0] as Message).text).toBe(result);
  f.receive({ type: 'transcript.final', role: 'assistant', origin: 'announcement', responseId: 'memory-speech', taskIds: ['memory'], content: result });
  f.stream.audio('memory-speech', 7, 40);
  f.receive({ type: 'audio.done', responseId: 'memory-speech' });
  f.stream.tick(8);
  const rows = voiceMessages(saved, f.state()) as Message[];
  expect(rows.map(row => row.streaming)).toEqual([false, false]);
  expect(rows[0].text).toBe(result);
  expect(f.value().messages[0].text).toBe(result);
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存实时读数' });
  expect(f.value().messages[0].text).toBe(result);
});

it('keeps text released by interruption complete when the task produces more output before history refresh', () => {
  const f = fixture();
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存' });
  f.stream.clear(); f.stream.tick(0);
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存实时读数，后续输出。' });
  expect(voiceMessages([], f.state()).map(row => ({ text: (row as Message).text, streaming: (row as Message).streaming })))
    .toEqual([{ text: '内存实时读数，后续输出。', streaming: false }]);
});

it('orders archived voice bubbles by the same timestamp displayed on them', () => {
  const f = fixture(), now = Date.now();
  f.receive({ type: 'task.output.delta', taskId: 'memory', content: '内存实时读数' });
  f.stream.clear(); f.stream.tick(0);
  const user = { ...f.message('', '再分析一下磁盘。'), id: 'disk-request', voiceKey: undefined,
    role: 'user' as const, createdAt: now + 1000, timestamp: '17:46:36' };
  const memory = { ...f.message('agent:memory', '内存实时读数'), createdAt: now + 4000, timestamp: '17:46:39' };
  const disk = { ...f.message('', '好，我把磁盘分析也加上。'), id: 'disk-reply', voiceKey: undefined,
    createdAt: now + 5000, timestamp: '17:46:40' };
  const rows = voiceMessages([user, memory, disk], f.state()) as Message[];
  expect(rows.map(row => row.text)).toEqual([user.text, memory.text, disk.text]);
  expect(rows.map(row => row.createdAt)).toEqual([user.createdAt, memory.createdAt, disk.createdAt]);
  expect(rows.map(row => row.timestamp)).toEqual(['17:46:36', '17:46:39', '17:46:40']);
});

it('projects delegated task activity immediately above its spoken result', () => {
  const f = fixture();
  f.receive({ type: 'task.snapshot', tasks: [{
    id: 'delegate', scene: { scene_id: 'scene', scene_meta: { scene_label: '当前场景' } },
    status: 'running', objective: '查询任务状态', startedAt: 10,
    activity: [
      { type: 'think', id: 'think-1', text: '正在分析', done: true },
      { type: 'tool', id: 'tool-1', name: '查询任务', inputDetail: '{"scope":"all"}', done: true,
        result: '已完成', resultDetail: '任务列表返回 2 项' },
    ],
  }] });
  f.receive({ type: 'transcript.final', role: 'assistant', origin: 'announcement', responseId: 'reply', taskIds: ['delegate'], content: '任务结果已返回。' });
  f.stream.audio('reply', 0, 1); f.receive({ type: 'audio.done', responseId: 'reply' });
  f.stream.ended('reply'); f.stream.tick(2);
  const rows = voiceMessages([f.message('agent:delegate', '任务结果已返回。')], f.state());
  expect(rows.map(row => row.kind)).toEqual(['run', 'message']);
  expect(rows[0].kind === 'run' && rows[0].entries[1].name).toBe('查询任务');
});

it('projects voice tool calls above the response that owns them', () => {
  const f = fixture();
  f.receive({ type: 'tool.call', callId: 'call-1', responseId: 'reply', name: 'get_agent_task_status', status: 'running', arguments: { task_id: 'opd' } });
  f.receive({ type: 'tool.call', callId: 'call-1', responseId: 'reply', name: 'get_agent_task_status', status: 'completed', result: '{"status":"running"}' });
  f.receive({ type: 'response.started', responseId: 'reply', origin: 'model' });
  f.receive({ type: 'transcript.final', role: 'assistant', responseId: 'reply', content: '正在帮你查询。' });
  f.stream.audio('reply', 0, 1); f.receive({ type: 'audio.done', responseId: 'reply' });
  f.stream.ended('reply'); f.stream.tick(2);
  const rows = voiceMessages([f.message('voice:reply', '正在帮你查询。')], f.state());
  expect(rows.map(row => row.kind)).toEqual(['run', 'message']);
  expect(rows[0].kind === 'run' && rows[0].entries[0].name).toBe('get_agent_task_status');
});
