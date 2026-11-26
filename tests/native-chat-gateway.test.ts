import { afterEach, expect, it, vi } from 'vitest';
import { mkdtempSync, mkdirSync, rmSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { parseConnection } from '../desktop/main/chat/connection';
import { NativeChatGateway } from '../desktop/main/chat/native-gateway';
import { ChatProxy } from '../desktop/main/chat/proxy';

const connection = parseConnection('https://example.org/willow/?token=fixture-token');
const scene = { scene_id: 'scene-1', scene_meta: { client: 'desktop', scene_label: 'First' } };
const frame = (event: string, data: unknown) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
const frames =
  frame('meta', { stream_id: 'being-stream' }) +
  frame('content_block_delta', { delta: { text: '答案' } }) +
  frame('meta', { persisted: { kind: 'self', seq: 999 } }) +
  frame('message_stop', {});
const cleanups: (() => void)[] = [];
afterEach(() => {
  for (const clean of cleanups.splice(0).reverse()) clean();
});

function fixture(
  fetcher: typeof fetch,
  options: Partial<ConstructorParameters<typeof NativeChatGateway>[0]> = {},
) {
  const directory = mkdtempSync(path.join(tmpdir(), 'native-chat-'));
  const gateway = new NativeChatGateway({
    directory,
    binding: () => ({ scene, connection }),
    fetcher,
    ...options,
  });
  const proxy = new ChatProxy(
    () => connection,
    (url, init) => gateway.fetchChat(url, init),
    () => scene,
  );
  proxy.localHistory = (route, init) => gateway.localHistory(route, init);
  cleanups.push(
    () => rmSync(directory, { recursive: true, force: true }),
    () => gateway.dispose(),
  );
  const get = (route: string) => proxy.handle(new Request('beings://chat/api/' + route));
  const post = (route: string, body: unknown) =>
    proxy.handle(
      new Request('beings://chat/api/' + route, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      }),
    );
  const send = (id: string) => post('chat/stream', { message: '问题', client_request_id: id });
  return { gateway, directory, get, post, send };
}

it('delivers reply receipts with local seqs, replays local events, and never reposts an accepted id', async () => {
  const upstream = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    expect(JSON.parse(String(init?.body))).toEqual({ ...scene, message: '问题' });
    return new Response(frames, { headers: { 'content-type': 'text/event-stream; charset=utf-8' } });
  });
  const { get, send, gateway } = fixture(upstream);
  const response = await send('abc');
  const text = await response.text();
  const { messages } = await (await get('history')).json();
  expect(messages.map((r: { content: string }) => r.content)).toEqual(['问题', '答案']);
  const { events, ...active } = await (await get('stream/active?after=1')).json();
  expect(active).toMatchObject({ finished: true, scene_id: scene.scene_id, next_seq: 4 });
  expect(text).toContain(active.stream_id);
  expect(events.map((e: { data: { persisted: { seq: number } } }) => e.data.persisted.seq)).toEqual([
    messages[1].seq,
    messages[1].seq,
  ]);
  gateway.dispose();
  expect((await send('abc')).status).toBe(202);
  expect(upstream).toHaveBeenCalledTimes(1);
});

it('reopens a Python-format SQLite archive, migrates indexed rows once, and preserves a scene cursor', async () => {
  const { gateway, directory, get, post } = fixture(async () => new Response(null, { status: 204 }));
  const scope = createHash('sha256').update(connection.endpoint).digest('hex');
  const folder = path.join(directory, 'gateway', scope);
  mkdirSync(folder, { recursive: true });
  const db = new DatabaseSync(path.join(folder, 'history.sqlite3'));
  db.exec(
    'CREATE TABLE history(seq INTEGER PRIMARY KEY AUTOINCREMENT, identity TEXT UNIQUE NOT NULL, scene TEXT NOT NULL, body TEXT NOT NULL)',
  );
  db.prepare('INSERT INTO history VALUES(41,?,?,?)').run(
    'voice:old:row',
    scene.scene_id,
    JSON.stringify({
      scene_id: scene.scene_id,
      scene_label: 'First',
      role: 'assistant',
      content: '旧语音',
      at: '2024-01-01',
      voiceKey: 'row',
    }),
  );
  db.close();
  const legacy = {
    messages: [{ role: 'user', scene_id: scene.scene_id, scene_label: 'First', content: '旧文字', seq: 99 }],
    complete: true,
  };
  expect((await (await post('local-history/import', legacy)).json()).complete).toBe(true);
  await post('local-history/import', legacy);
  gateway.recordVoice(
    'voice:new:row',
    { scene_id: scene.scene_id, scene_label: 'First', role: 'user', content: '新语音' },
    connection.endpoint,
  );
  const page = await (await get('local-history?after=41')).json();
  expect(page.messages.map((r: { content: string }) => r.content)).toEqual(['旧文字', '新语音']);
  expect((await (await get('local-history?before=42')).json()).messages[0]).toMatchObject({
    seq: 41,
    voiceKey: 'row',
  });
  expect((await (await get('local-history/scenes')).json()).scenes[0]).toMatchObject({
    sceneId: scene.scene_id,
    messageCount: 3,
  });
});

it('recovers a disconnected native stream from its durable upstream cursor without sending twice', async () => {
  let sends = 0;
  const fetcher: typeof fetch = async (input, init) => {
    const url = new URL(String(input));
    expect(url.searchParams.get('token')).toBe(connection.token);
    if (init?.method === 'POST') {
      sends++;
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(
              new TextEncoder().encode(
                frame('meta', { stream_id: 'up' }) + frame('content_block_delta', { delta: { text: '先' } }),
              ),
            );
          },
        }),
        { headers: { 'content-type': 'text/event-stream' } },
      );
    }
    expect(url.searchParams.get('after')).toBe('1');
    return Response.json({
      stream_id: 'up',
      finished: true,
      events: [
        { event: 'content_block_delta', seq: 2, data: { delta: { text: '后' } } },
        { event: 'message_stop', seq: 3, data: {} },
      ],
    });
  };
  const { gateway, send, get } = fixture(fetcher);
  const reader = (await send('one')).body!.getReader();
  let text = '';
  while (!text.includes('先')) text += new TextDecoder().decode((await reader.read()).value);
  await reader.cancel();
  gateway.dispose();
  gateway.ensure();
  await expect.poll(async () => (await (await get('history')).json()).messages.at(-1).content).toBe('先后');
  expect(sends).toBe(1);
});

it('forwards upstream admission errors unchanged and allows an explicit retry only for a rejected send', async () => {
  const fetcher = vi.fn(
    async () => new Response('原始错误', { status: 429, headers: { 'content-type': 'text/plain' } }),
  );
  const { send } = fixture(fetcher);
  for (let i = 0; i < 2; i++) {
    const response = await send('retry');
    expect(response.status).toBe(429);
    expect(response.headers.get('content-type')).toBe('text/plain');
    expect(await response.text()).toBe('原始错误');
  }
  expect(fetcher).toHaveBeenCalledTimes(2);
});

it('keeps recent confirmed voice context and pending tasks scoped to the original Being and scene', () => {
  const { gateway } = fixture(async () => new Response(null, { status: 204 }));
  for (let i = 0; i < 21; i++)
    gateway.recordVoice(
      'voice:' + i,
      { scene_id: scene.scene_id, role: 'assistant', content: String(i) },
      connection.endpoint,
    );
  gateway.recordVoice(
    'partial',
    { scene_id: scene.scene_id, role: 'assistant', content: 'partial', voicePending: true },
    connection.endpoint,
  );
  gateway.recordVoice(
    'other',
    { scene_id: 'scene-2', role: 'assistant', content: 'other' },
    connection.endpoint,
  );
  gateway.saveVoiceTasks(connection.endpoint, scene.scene_id, [{ id: 'old' }]);
  gateway.saveVoiceTasks('https://other.example.test', scene.scene_id, []);
  gateway.dispose();
  expect(gateway.recentVoiceContext(connection.endpoint, scene.scene_id).map((row) => row.content)).toEqual(
    Array.from({ length: 16 }, (_, i) => String(i + 5)),
  );
  expect(gateway.readVoiceTasks(connection.endpoint, scene.scene_id)).toEqual([{ id: 'old' }]);
  expect(gateway.readVoiceTasks(connection.endpoint, 'scene-2')).toBeUndefined();
});

it('requires the child control tool receipt even when the parent stream has already finished', async () => {
  let verified = false;
  const onSend = vi.fn();
  const { send, get, post } = fixture(
    async (_input, init) => {
      const control = JSON.parse(String(init?.body)).message.includes('portal_subagent_control');
      const body = control
        ? verified
          ? frame('tool_use', {
              id: 'cancel',
              name: 'act',
              input: { body: 'portal_subagent_control --task sub_child' },
            }) +
            frame('tool_result', {
              id: 'cancel',
              content: [{ type: 'text', text: '{"task_id":"sub_child","status":"cancelled"}' }],
            })
          : frame('content_block_delta', { delta: { text: '已经取消' } })
        : frame('meta', { stream_id: 'up' }) +
          frame('tool_use', { id: 'spawn', name: 'portal_subagent_spawn' }) +
          frame('tool_result', { id: 'spawn', result: { task_id: 'sub_child', status: 'queued' } }) +
          frame('message_stop', {});
      return new Response(body, { headers: { 'content-type': 'text/event-stream' } });
    },
    { onSend },
  );
  await (await send('id')).text();
  const active = await (await get('stream/active')).json();
  expect(active).toMatchObject({ finished: true, awaiting_subagents: true });
  expect((await post('stop', { stream_id: active.stream_id })).status).toBe(409);
  verified = true;
  expect((await post('stop', { stream_id: active.stream_id })).status).toBe(200);
  expect(await (await get('stream/active')).json()).toMatchObject({ finished: true, stopped: true });
  await send('id');
  expect(onSend).toHaveBeenCalledTimes(1);
});

it('joins a 202 stream using the original scene and never resubmits a duplicate', async () => {
  let sends = 0;
  const { send, get } = fixture(async (_input, init) => {
    if (init?.method === 'POST') {
      sends++;
      return new Response('已进入队列', { status: 202, headers: { 'content-type': 'text/plain' } });
    }
    return Response.json({
      stream_id: 'active',
      scene_id: scene.scene_id,
      finished: true,
      events: [
        { seq: 1, event: 'content_block_delta', data: { delta: { text: '已完成' } } },
        { seq: 2, event: 'message_stop', data: {} },
      ],
    });
  });
  const admitted = await send('once');
  expect(await admitted.text()).toBe('已进入队列');
  await expect.poll(async () => (await (await get('history')).json()).messages.at(-1).content).toBe('已完成');
  expect((await send('once')).status).toBe(202);
  expect(sends).toBe(1);
});

it('correlates a background callback while a human stream owns the replay window', async () => {
  const id = 'task_' + 'a'.repeat(32);
  const onChildResult = vi.fn(() => true);
  const { send, get } = fixture(
    async (_input, init) => {
      if (init?.method === 'POST')
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(frame('meta', { stream_id: 'human' })));
            },
          }),
          { headers: { 'content-type': 'text/event-stream' } },
        );
      return Response.json({
        origin: 'callback',
        stream_id: 'cb',
        scene_id: scene.scene_id,
        finished: true,
        events: [
          { seq: 1, event: 'content_block_delta', data: { delta: { text: '<!-- gateway-' } } },
          { seq: 2, event: 'content_block_delta', data: { delta: { text: 'task: ' + id + ' -->\n结果' } } },
          { seq: 3, event: 'message_stop', data: {} },
        ],
      });
    },
    { onChildResult },
  );
  const body = (await send('human')).body!;
  const active = await (await get('stream/active')).json();
  await vi.waitFor(() => expect(onChildResult).toHaveBeenCalledWith(scene.scene_id, [id], 'cb', '结果', {}), {
    timeout: 5000,
    interval: 60,
  });
  expect((await (await get('stream/active')).json()).stream_id).toBe(active.stream_id);
  expect((await (await get('history')).json()).messages).toHaveLength(1);
  await body.cancel();
});

it('checks the original stream, scene and request order when stopping typed work from voice', async () => {
  let stopCalls = 0;
  const { gateway, send } = fixture(async (input, init) => {
    const url = new URL(String(input));
    if (url.pathname.endsWith('/api/stop')) {
      stopCalls++;
      expect(JSON.parse(String(init?.body))).toEqual({ stream_id: 'up' });
      return Response.json({ ok: true, stream_id: 'up' });
    }
    if (JSON.parse(String(init?.body)).message === '问题')
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(frame('meta', { stream_id: 'up' })));
          },
        }),
        { headers: { 'content-type': 'text/event-stream' } },
      );
    throw new Error('unexpected request');
  });
  const response = await send('x'),
    reader = response.body!.getReader();
  await reader.read();
  const work = gateway.activeText(scene.scene_id)!;
  expect(gateway.activeText('other')).toBeUndefined();
  expect((await gateway.stopText('other', work.stream_id, work.order)).status).toBe(409);
  expect((await gateway.stopText(scene.scene_id, work.stream_id, work.order - 1)).status).toBe(409);
  expect((await gateway.stopText(scene.scene_id, work.stream_id, work.order)).status).toBe(200);
  expect(gateway.activeText(scene.scene_id)).toBeUndefined();
  expect(stopCalls).toBe(1);
  await reader.cancel();
});

it('rejects an IndexedDB import whose Being changed during the body read', async () => {
  let current = connection,
    release!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const request = new Request('beings://chat/api/local-history/import', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'X-Portal-Being-Endpoint': current.endpoint },
    body: '{"messages":[]}',
  });
  const original = request.text.bind(request);
  vi.spyOn(request, 'text').mockImplementation(async () => {
    await blocked;
    return original();
  });
  const proxy = new ChatProxy(
    () => current,
    fetch,
    () => scene,
  );
  proxy.localHistory = vi.fn(async () => Response.json({ ok: true }));
  const pending = proxy.handle(request);
  current = parseConnection('https://example.org/other/?token=second');
  release();
  expect((await pending).status).toBe(409);
  expect(proxy.localHistory).not.toHaveBeenCalled();
});

it('migrates Python notes, alarms and receipt-backed work once for the existing pairing', () => {
  const second = { ...scene, scene_id: 'two' };
  const { gateway, directory } = fixture(async () => new Response(null, { status: 204 }), {
    scenes: () => [scene, second],
  });
  const scope = createHash('sha256').update(connection.endpoint).digest('hex');
  const account = createHash('sha256').update(connection.token).digest('hex') + ':';
  const folder = path.join(directory, 'gateway', scope);
  mkdirSync(folder, { recursive: true });
  const db = new DatabaseSync(path.join(folder, 'gateway.sqlite3'));
  db.exec('CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT NOT NULL)');
  const put = db.prepare('INSERT INTO kv VALUES(?,?)');
  put.run('notes_v2', JSON.stringify({ 购物: { items: ['茶'], updated_at: 123 } }));
  const base = { objective: '旧任务', scene: second, createdAt: 100, activity: [] };
  put.run(
    'tasks',
    JSON.stringify({
      alarm: {
        ...base,
        id: 'alarm',
        status: 'scheduled',
        schedule: {
          type: 'reminder',
          executeAt: 9999999999999,
          recurrence: 'daily',
          seriesId: 'series',
          timeZone: 'Asia/Shanghai',
        },
      },
      unsafe: { ...base, id: 'unsafe', status: 'running', being: { submitted: true } },
      queued: { ...base, id: 'queued', status: 'queued' },
      recover: {
        ...base,
        id: 'recover',
        status: 'recovering',
        being: { submitted: true, endpoint: connection.endpoint, scene_id: 'two', stream_id: 'upstream' },
      },
      finished: { ...base, id: 'finished', status: 'completed', result: '旧结果' },
    }),
  );
  db.close();
  const notesKey = JSON.stringify([account + scene.scene_id, 'notes_v2']);
  expect(gateway.readVoiceTasks(connection.endpoint, notesKey)).toEqual({
    购物: { items: ['茶'], updated_at: 123 },
  });
  expect(
    gateway.readVoiceTasks(connection.endpoint, JSON.stringify([account + 'two', 'notes_v2'])),
  ).toBeUndefined();
  const tasks = gateway.readVoiceTasks(connection.endpoint, account + 'two') as {
    id: string;
    status: string;
    error?: string;
  }[];
  expect(tasks).toHaveLength(5);
  expect(tasks.find((t) => t.id === 'alarm')).toMatchObject({
    status: 'scheduled',
    schedule: { seriesId: 'series', executeAt: 9999999999999 },
  });
  expect(tasks.find((t) => t.id === 'recover')).toMatchObject({
    status: 'running',
    submitted: true,
    streamId: 'upstream',
  });
  expect(tasks.find((t) => t.id === 'finished')).toMatchObject({ status: 'completed', result: '旧结果' });
  for (const id of ['unsafe', 'queued'])
    expect(tasks.find((t) => t.id === id)).toMatchObject({
      status: 'failed',
      error: expect.stringContaining('核对'),
    });
  gateway.saveVoiceTasks(connection.endpoint, notesKey, {});
  gateway.dispose();
  gateway.ensure();
  expect(gateway.readVoiceTasks(connection.endpoint, notesKey)).toEqual({});
  expect(gateway.readVoiceTasks(connection.endpoint, account + 'two')).toHaveLength(5);
});

it('keeps unscoped old notes out of a new pairing introduced after startup', () => {
  let bound: { connection: typeof connection; scene: typeof scene } | null = null;
  const { gateway, directory } = fixture(async () => new Response(null, { status: 204 }), {
    binding: () => bound,
  });
  const folder = path.join(
    directory,
    'gateway',
    createHash('sha256').update(connection.endpoint).digest('hex'),
  );
  mkdirSync(folder, { recursive: true });
  const db = new DatabaseSync(path.join(folder, 'gateway.sqlite3'));
  db.exec('CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT NOT NULL)');
  db.prepare('INSERT INTO kv VALUES(?,?)').run('notes', '{"old":["private"]}');
  db.close();
  bound = { connection, scene };
  const account = createHash('sha256').update(connection.token).digest('hex') + ':' + scene.scene_id;
  expect(gateway.readVoiceTasks(connection.endpoint, JSON.stringify([account, 'notes_v2']))).toBeUndefined();
});
