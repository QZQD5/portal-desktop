import { afterEach, expect, it, vi } from 'vitest';
import { VoiceTasks } from '../desktop/shared/voice-tasks';
import type { VoiceToolCall } from '../desktop/shared/voice-realtime';
import type { ChatScene } from '../desktop/shared/types';

const scene: ChatScene = { scene_id: 'room', scene_meta: { client: 'desktop', scene_label: '工作' } };
const frame = (event: string, data: unknown) =>
  'event: ' + event + '\ndata: ' + JSON.stringify(data) + '\n\n';
const sse = (...frames: string[]) =>
  new Response(frames.join(''), { headers: { 'Content-Type': 'text/event-stream' } });
const idle = () => new Response(null, { status: 204 });
const content = (text: string) => frame('content_block_delta', { delta: { text } });
const childSpawn = [
  frame('meta', { stream_id: 'parent', scene_id: 'room' }),
  frame('tool_use', { id: 'spawn', name: 'portal_subagent_spawn' }),
  frame('tool_result', { id: 'spawn', result: { task_id: 'sub_child', status: 'running' } }),
  content('交给子代理处理了。'),
  frame('message_stop', {}),
];
const tool = (name: string, args: VoiceToolCall['args'], targets?: readonly string[]): VoiceToolCall => ({
  id: crypto.randomUUID(),
  name,
  args,
  responseId: 'model',
  turnId: crypto.randomUUID(),
  transcript: '用户本轮原话',
  targets,
});
const record = (status: string, overrides: object = {}) => ({
  status,
  id: 'job',
  turnId: 'u',
  scene,
  objective: '做事',
  message: '做事',
  createdAt: 1,
  result: '',
  activity: [],
  ...overrides,
});
const fixture = (request: (path: string, init: RequestInit) => Promise<Response>, saved: unknown = []) => {
  const speak = vi.fn(),
    emit = vi.fn(),
    post = vi.fn(request);
  const options = {
    request: post,
    scene: () => scene,
    load: () => saved,
    save: (rows: unknown) => {
      saved = structuredClone(rows);
    },
    speak,
    emit,
  };
  return { options, speak, emit, post };
};
afterEach(() => vi.useRealTimers());

it('keeps old pending results in history without restarting uncertain speech', () => {
  const f = fixture(async () => idle(), [
    record('completed', { notificationStatus: 'pending', result: '原任务结果' }),
    record('completed', { id: 'new', notificationStatus: 'queued', result: '新到的通知' }),
  ]);
  const onTask = vi.fn();
  const tasks = new VoiceTasks({ ...f.options, onTask });
  try {
    tasks.resume();
    tasks.resume();
    expect(f.speak).toHaveBeenCalledExactlyOnceWith('新到的通知', ['new']);
    expect(onTask).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({
      id: 'job', result: '原任务结果', status: 'completed', notificationStatus: 'interrupted',
    }));
    expect(f.options.load()).toMatchObject([
      { notificationStatus: 'interrupted', result: '原任务结果' }, { notificationStatus: 'queued' },
    ]);
  } finally {
    tasks.dispose();
  }
});

it('does not replay an explicitly interrupted announcement after a late receipt, hangup or app restart', () => {
  const f = fixture(async () => idle(), [
    record('completed', { notificationStatus: 'queued', result: '长回复' }),
    record('completed', { id: 'second', notificationStatus: 'queued', result: '同轮排队的回复' }),
  ]);
  let tasks = new VoiceTasks(f.options);
  try {
    tasks.resume();
    tasks.playback({ type: 'playback.requested', taskIds: ['job'] });
    tasks.playback({ type: 'playback.cancelled', reason: 'user_interruption', taskIds: ['job', 'second'] });
    tasks.playback({ type: 'playback.ended', taskIds: ['job'] });
    expect(f.options.load()).toMatchObject([
      { notificationStatus: 'interrupted', result: '长回复' },
      { notificationStatus: 'interrupted', result: '同轮排队的回复' },
    ]);
    tasks.suspend();
    tasks.resume();
    tasks.dispose();
    tasks = new VoiceTasks(f.options);
    tasks.resume();
    expect(f.speak).toHaveBeenCalledTimes(2);
  } finally {
    tasks.dispose();
  }
});

it('reoffers only never-submitted audio after the transport reconnects', () => {
  const f = fixture(async () => idle(), [
    record('completed', { notificationStatus: 'queued', result: '说到一半' }),
    record('completed', { id: 'second', notificationStatus: 'queued', result: '还未发送' }),
  ]);
  const tasks = new VoiceTasks(f.options);
  try {
    tasks.resume();
    tasks.playback({ type: 'playback.requested', taskIds: ['job'] });
    expect(f.options.load()).toMatchObject([{ notificationStatus: 'pending' }, { notificationStatus: 'queued' }]);
    tasks.playback({ type: 'playback.cancelled', taskIds: ['second'], reason: 'disconnected' });
    tasks.suspend(); // The socket closed before the first announcement's final receipt.
    tasks.resume();
    tasks.playback({ type: 'playback.requested', taskIds: ['second'] });
    tasks.playback({ type: 'playback.ended', taskIds: ['second'] });
    expect(f.speak.mock.calls.map(([text]) => text)).toEqual(['说到一半', '还未发送', '还未发送']);
    expect(f.options.load()).toMatchObject([{ notificationStatus: 'interrupted' }, { notificationStatus: 'delivered' }]);
  } finally {
    tasks.dispose();
  }
});

it('releases the SSE reader after a confirmed stop so the next work can start', async () => {
  let active = false,
    posted = 0;
  const cancel = vi.fn();
  const f = fixture(async (path, init) => {
    if (path === '/api/stop') {
      active = false;
      return idle();
    }
    if (init.method !== 'POST')
      return active ? Response.json({ stream_id: 'first', scene_id: 'room' }) : idle();
    if (posted++) return sse(content('第二项结果'), frame('message_stop', {}));
    active = true;
    return new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(
            new TextEncoder().encode(frame('meta', { stream_id: 'first', scene_id: 'room' })),
          );
        },
        cancel,
      }),
      { headers: { 'Content-Type': 'text/event-stream' } },
    );
  });
  const tasks = new VoiceTasks(f.options);
  try {
    const first = (await tasks.run(tool('spawn_thinking', { objective: '第一项' }))) as { task_id: string };
    await tasks.run(tool('spawn_thinking', { objective: '第二项' }));
    await vi.waitFor(() =>
      expect(f.options.load()).toMatchObject([{ streamId: 'first' }, { status: 'queued' }]),
    );
    expect(await tasks.run(tool('cancel_agent_task', { task_id: first.task_id }))).toMatchObject({
      status: 'cancelled',
    });
    await vi.waitFor(() =>
      expect(f.options.load()).toMatchObject([
        { status: 'cancelled' },
        { status: 'completed', result: '第二项结果' },
      ]),
    );
    expect(cancel).toHaveBeenCalledTimes(1);
  } finally {
    tasks.dispose();
  }
});

it('persists child receipts and waits for a positively attributed terminal result after restart', async () => {
  const f = fixture(async (_path, init) => (init.method === 'POST' ? sse(...childSpawn) : idle()));
  let tasks = new VoiceTasks({ ...f.options, callbackHost: true });
  try {
    tasks.resume();
    const result = (await tasks.run(tool('spawn_thinking', { objective: '整理资料' }))) as {
      task_id: string;
    };
    await vi.waitFor(() =>
      expect(f.options.load()).toMatchObject([
        { status: 'delegated', being: { subagents: { sub_child: 'running' } } },
      ]),
    );
    expect(f.speak).not.toHaveBeenCalled();
    const submitted = f.post.mock.calls.find(([, init]) => init.method === 'POST')!;
    expect(JSON.parse(String(submitted[1].body)).message).toContain(
      '<!-- gateway-task: ' + result.task_id + ' -->',
    );
    tasks.dispose();
    tasks = new VoiceTasks({ ...f.options, callbackHost: true });
    tasks.resume();
    expect(tasks.childResult('another', ['sub_child'], 'cb', '错误场景', { sub_child: 'done' })).toBe(false);
    expect(tasks.childResult('', [result.task_id], 'cb', '还未核实', {})).toBe(false);
    expect(tasks.childResult('', [result.task_id], 'cb', '整理完成。', { sub_child: 'done' })).toBe(true);
    expect(tasks.childResult('', [result.task_id], 'cb', '整理完成。', { sub_child: 'done' })).toBe(true);
    expect(f.speak).toHaveBeenCalledExactlyOnceWith('整理完成。', [result.task_id]);
    expect(f.post.mock.calls.filter(([, init]) => init.method === 'POST')).toHaveLength(1);
    tasks.playback({ type: 'playback.requested', taskIds: [result.task_id] });
    expect(f.options.load()).toMatchObject([{ notificationStatus: 'pending' }]);
    tasks.playback({ type: 'playback.ended', taskIds: [result.task_id] });
    expect(f.options.load()).toMatchObject([{ notificationStatus: 'delivered' }]);
  } finally {
    tasks.dispose();
  }
});

it('finds a completed mobile callback from a verified child receipt when no desktop callback host exists', async () => {
  vi.useFakeTimers();
  const f = fixture(
    async () =>
      Response.json({
        origin: 'callback',
        stream_id: 'report',
        finished: true,
        events: [
          { seq: 1, event: 'content_block_delta', data: { delta: { text: '已办好。' } } },
          { seq: 2, event: 'tool_use', data: { id: 'status', name: 'portal_subagent_status' } },
          {
            seq: 3,
            event: 'tool_result',
            data: { id: 'status', result: { task_id: 'sub_child', status: 'done' } },
          },
          { seq: 4, event: 'message_stop', data: {} },
        ],
      }),
    [
      record('delegated', {
        being: { subagents: { sub_child: 'running' } },
        submitted: true,
        parentFinished: true,
      }),
    ],
  );
  const tasks = new VoiceTasks(f.options);
  try {
    tasks.resume();
    await vi.advanceTimersByTimeAsync(1510);
    expect(f.speak).toHaveBeenCalledExactlyOnceWith('已办好。', ['job']);
    await vi.advanceTimersByTimeAsync(8000);
    expect(f.post).toHaveBeenCalledTimes(1);
  } finally {
    tasks.dispose();
  }
});

it('requires actual child stop receipts and never stops another scene or a newly spliced text message', async () => {
  let confirmed = false,
    order = 1;
  const stopText = vi.fn(async () => idle());
  const f = fixture(
    async (_path, init) => {
      expect(init.method).toBe('POST');
      return confirmed
        ? sse(
            frame('tool_use', { id: 'cancel', name: 'portal_subagent_control' }),
            frame('tool_result', { id: 'cancel', result: { task_id: 'sub_child', status: 'cancelled' } }),
          )
        : sse(content('已经取消'));
    },
    [
      record('delegated', {
        being: { subagents: { sub_child: 'running' } },
        submitted: true,
        parentFinished: true,
      }),
      record('delegated', { id: 'foreign', scene: { scene_id: 'elsewhere' } }),
    ],
  );
  const tasks = new VoiceTasks({
    ...f.options,
    callbackHost: true,
    textWork: () => ({ scene_id: 'room', stream_id: 'text', status: 'running', order }),
    stopText,
  });
  try {
    const scope = tasks.cancellableIds();
    expect(await tasks.run(tool('cancel_agent_task', {}, scope))).toMatchObject({ status: 'error' });
    expect(await tasks.run(tool('cancel_agent_task', { task_id: 'foreign' }, scope))).toMatchObject({
      status: 'error',
    });
    expect(f.post).not.toHaveBeenCalled();
    expect(await tasks.run(tool('cancel_agent_task', { task_id: 'job' }, scope))).toMatchObject({
      status: 'error',
    });
    expect(tasks.childStates()).toEqual({ sub_child: 'running' });
    confirmed = true;
    expect(await tasks.run(tool('cancel_agent_task', { task_id: 'job' }, scope))).toMatchObject({
      status: 'cancelled',
    });
    expect(tasks.childResult('', ['sub_child'], 'late', '迟到的结果', { sub_child: 'done' })).toBe(true);
    expect(f.speak).not.toHaveBeenCalled();
    order++;
    expect(await tasks.run(tool('cancel_agent_task', { stream_id: 'text' }, scope))).toMatchObject({
      status: 'error',
    });
    expect(stopText).not.toHaveBeenCalled();
    expect(
      await tasks.run(tool('cancel_agent_task', { stream_id: 'text' }, tasks.cancellableIds())),
    ).toMatchObject({ status: 'cancelled' });
    expect(stopText).toHaveBeenCalledExactlyOnceWith('text', 2);
  } finally {
    tasks.dispose();
  }
});

it('recovers the original stream with contiguous replay, skips unrelated scene continuations, and never repeats the POST', async () => {
  let reads = 0;
  const f = fixture(async () => {
    if (!reads++) throw new TypeError('network unavailable');
    return Response.json({
      stream_id: 'existing',
      finished: true,
      events: [
        { seq: 1, event: 'meta', data: { scene_id: 'elsewhere' } },
        { seq: 2, event: 'content_block_delta', data: { delta: { text: '其他场景' } } },
        { seq: 3, event: 'meta', data: { scene_id: 'room' } },
        { seq: 4, event: 'content_block_delta', data: { delta: { text: '恢复结果' } } },
        { seq: 5, event: 'message_stop', data: {} },
      ],
    });
  }, [record('running', { streamId: 'existing', submitted: true, result: '半截' })]);
  const tasks = new VoiceTasks(f.options);
  try {
    tasks.resume();
    await vi.waitFor(
      () => expect(f.options.load()).toMatchObject([{ status: 'completed', result: '恢复结果' }]),
      { timeout: 2500 },
    );
    expect(f.post.mock.calls.map(([url]) => url)).toEqual([
      '/api/stream/active?after=0',
      '/api/stream/active?after=0',
    ]);
    expect(f.speak).toHaveBeenCalledExactlyOnceWith('恢复结果', ['job']);
  } finally {
    tasks.dispose();
  }
});

it('does not claim completion from a missing replay frame or resend a submission with no stream receipt', async () => {
  const f = fixture(
    async () =>
      Response.json({
        stream_id: 'seen',
        finished: true,
        events: [{ seq: 4, event: 'content_block_delta', data: { delta: { text: '残缺结果' } } }],
      }),
    [
      record('running', { streamId: 'seen', submitted: true }),
      record('running', { id: 'unconfirmed', submitted: true }),
    ],
  );
  const tasks = new VoiceTasks(f.options);
  try {
    await vi.waitFor(() =>
      expect(f.options.load()).toMatchObject([{ status: 'failed' }, { status: 'failed' }]),
    );
    expect(f.post).toHaveBeenCalledTimes(1);
    expect(f.post.mock.calls[0][0]).toBe('/api/stream/active?after=0');
  } finally {
    tasks.dispose();
  }
});
