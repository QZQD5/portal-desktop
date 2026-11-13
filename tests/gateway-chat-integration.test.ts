import { expect, it } from 'vitest';
import { createServer } from 'node:http';
import { once } from 'node:events';
import type { ChildProcess } from 'node:child_process';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { VoiceGateway } from '../desktop/main/voice/gateway';
import { ChatProxy } from '../desktop/main/chat/proxy';
import { parseConnection } from '../desktop/main/chat/connection';

it.skipIf(!process.env.PORTAL_DESKTOP_GATEWAY_ROOT)('routes desktop text through a real Python child and retains local history without voice or cloud access', async () => {
  const submitted: any[] = [];
  const historyReads: string[] = [], recoveryCursors: string[] = [];
  let active: any = null;
  const server = createServer(async (request, response) => {
    const url = new URL(request.url!, 'http://localhost');
    expect(url.searchParams.get('token')).toBe('fixture-token');
    if (url.pathname.endsWith('/api/status')) { response.setHeader('Content-Type', 'application/json'); response.end('{}'); return; }
    if (url.pathname.endsWith('/api/stream/active')) {
      if (!active) { response.writeHead(204).end(); return; }
      if (url.searchParams.has('after')) recoveryCursors.push(url.searchParams.get('after')!);
      response.setHeader('Content-Type', 'application/json');
      response.end(JSON.stringify({ ...active, events: active.events.filter((e: any) => e.seq > Number(url.searchParams.get('after') || 0)) }));
      return;
    }
    if (url.pathname.endsWith('/api/history')) { historyReads.push(url.href); response.writeHead(500).end(); return; }
    if (url.pathname.endsWith('/api/chat/stream')) {
      let body = ''; for await (const chunk of request) body += chunk;
      submitted.push(JSON.parse(body));
      response.writeHead(200, { 'Content-Type': 'text/event-stream' });
      if (submitted.length === 3) {
        active = { stream_id: 'recover-me', scene_id: 'second', finished: true, events: [
          { seq: 1, event: 'content_block_delta', data: { delta: { text: '重启前' } } },
          { seq: 2, event: 'content_block_delta', data: { delta: { text: '继续完成' } } },
          { seq: 3, event: 'message_stop', data: {} },
        ] };
        response.write('event: meta\ndata: {"stream_id":"recover-me"}\n\nevent: content_block_delta\ndata: {"delta":{"text":"重启前"}}\n\n');
        return;
      }
      response.end('event: meta\ndata: {"stream_id":"remote"}\n\nevent: content_block_delta\ndata: {"delta":{"text":"本地归档结果"}}\n\nevent: message_stop\ndata: {}\n\n');
      return;
    }
    response.writeHead(500).end();
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as { port: number }).port;
  const connection = parseConnection(`http://127.0.0.1:${port}/being?token=fixture-token`);
  let scene = { scene_id: 'first', scene_meta: { client: 'desktop', scene_label: 'First' } };
  const directory = await mkdtemp(path.join(tmpdir(), 'portal-local-memory-'));
  const gateway = new VoiceGateway({ directory, resources: directory, packaged: false,
    binding: () => ({ connection, scene }), emit: () => {},
    env: { ...process.env, DOUBAO_API_KEY: '', PORTAL_DESKTOP_GATEWAY_URL: '' } });
  const proxy = new ChatProxy(() => connection, (url, init) => gateway.fetchChat(url, init), () => scene);
  try {
    await gateway.ensure();
    const send = (id: string) => proxy.handle(new Request('beings://chat/api/chat/stream', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: '文字请求', client_request_id: id }),
    }));
    expect(await (await send('one')).text()).toContain('本地归档结果');
    gateway.disconnect(); // Stopping voice cannot stop the chat service.
    scene = { scene_id: 'second', scene_meta: { client: 'desktop', scene_label: 'Second' } };
    await gateway.syncBinding();
    expect(await (await send('two')).text()).toContain('本地归档结果');
    expect(submitted.map(item => item.scene_id)).toEqual(['first', 'second']);
    const pending = await send('three');
    const reader = pending.body!.getReader();
    let partial = '';
    while (!partial.includes('重启前')) partial += new TextDecoder().decode((await reader.read()).value);
    await reader.cancel(); // Renderer disconnect does not cancel the remote task.
    const child = (gateway as unknown as { child: ChildProcess }).child;
    const exited = once(child, 'exit');
    child.kill(); // Only this fixture's child: exercise an ungraceful process exit.
    await exited;
    await gateway.ensure();
    await expect.poll(async () => {
      const result = await proxy.handle(new Request('beings://chat/api/history'));
      return (await result.json()).messages.at(-1)?.content;
    }).toBe('重启前继续完成');
    expect(recoveryCursors).toEqual(['1']);
    expect(submitted).toHaveLength(3);
    active = { stream_id: 'web-only', scene_id: 'web', finished: false, events: [
      { seq: 1, event: 'content_block_delta', data: { delta: { text: '网页私有消息' } } },
    ] };
    const localActive = await (await proxy.handle(new Request('beings://chat/api/stream/active'))).json();
    expect(localActive.stream_id).not.toBe('web-only');
    expect(JSON.stringify(localActive)).not.toContain('网页私有消息');
    await gateway.shutdown();
    await new Promise<void>(resolve => server.close(() => resolve()));
    const history = await proxy.handle(new Request('beings://chat/api/history'));
    expect(history.ok).toBe(true);
    expect((await history.json()).messages.map((row: any) => row.content)).toEqual([
      '文字请求', '本地归档结果', '文字请求', '本地归档结果',
      '文字请求', '重启前继续完成',
    ]);
    expect(historyReads).toEqual([]);
  } finally {
    await gateway.shutdown();
    server.closeAllConnections(); server.close();
  }
}, 60000);
