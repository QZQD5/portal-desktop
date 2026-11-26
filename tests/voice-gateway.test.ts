import { expect, it, vi } from 'vitest';
import { createServer } from 'node:http';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { WebSocketServer } from 'ws';
import { VoiceGateway, type GatewayOptions } from '../desktop/main/voice/gateway';
import { parseConnection } from '../desktop/main/chat/connection';
import { ChatSessions } from '../desktop/main/chat/scene';

const scene = { scene_id: 'fixture-scene', scene_meta: { client: 'desktop', scene_label: 'Fixture' } };
async function options(env: NodeJS.ProcessEnv): Promise<GatewayOptions> {
  const directory = await mkdtemp(path.join(tmpdir(), 'portal-voice-connection-'));
  return { directory, resources: directory, packaged: false, env,
    binding: () => ({ connection: parseConnection('https://being.example/fixture/?token=private-being'), scene }),
    emit: () => {},
  };
}

it.each([401, 429, 503])('handles HTTP %i from the realtime handshake', async status => {
  let attempts = 0;
  const server = createServer();
  const upstream = new WebSocketServer({ noServer: true });
  server.on('upgrade', (request, socket, head) => {
    attempts++;
    if (attempts === 1 || status !== 503) {
      socket.end(`HTTP/1.1 ${status} Test\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
      return;
    }
    upstream.handleUpgrade(request, socket, head, peer => peer.on('message', raw => {
      if (JSON.parse(raw.toString()).type === 'session.create') peer.send('{"type":"session.created"}');
    }));
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const opts = await options({
    PORTAL_DESKTOP_VOICE_PROVIDER_URL: `ws://127.0.0.1:${(server.address() as { port: number }).port}/`, DOUBAO_API_KEY: 'fixture',
  });
  const logConnection = vi.fn();
  const gateway = new VoiceGateway({ ...opts, logConnection });
  try {
    if (status === 503) {
      await gateway.start('dictation');
      expect(attempts).toBe(2);
    } else {
      await expect(gateway.start('dictation')).rejects.toThrow(`HTTP ${status}`);
      expect(attempts).toBe(1);
    }
    expect(logConnection).toHaveBeenCalledWith('gateway-error', expect.any(Number), status);
    expect(logConnection).toHaveBeenCalledWith(status === 503 ? 'session-ready' : 'session-failed', expect.any(Number));
  } finally {
    await gateway.shutdown();
    for (const peer of upstream.clients) peer.terminate();
    await new Promise<void>(resolve => upstream.close(() => resolve()));
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});

it('rebinds a restarted manual Gateway before retrying an unaccepted text request', async () => {
  let configured = false, bindings = 0, accepted = 0;
  const server = createServer(async (request, response) => {
    response.setHeader('Content-Type', 'application/json');
    if (request.url === '/api/dev/status') {
      response.end(JSON.stringify({ kind: 'audio-agent-gateway-dev', protocol_version: '7.0.0', local_history_version: 1, configured }));
    } else if (request.url === '/api/dev/configure') {
      for await (const _ of request) { /* Drain fixture configuration. */ }
      configured = true; bindings++; response.end('{"ok":true}');
    } else if (!configured) response.writeHead(503).end('{"error":"Waiting for Desktop configuration"}');
    else { accepted++; response.end('{"ok":true}'); }
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const gateway = new VoiceGateway(await options({ PORTAL_DESKTOP_GATEWAY_URL: `http://127.0.0.1:${(server.address() as { port: number }).port}` }));
  try {
    await gateway.ensure();
    configured = false; // Manual restart while no voice socket is open.
    const response = await gateway.request('/api/heart/chat/stream', { method: 'POST', body: '{}' });
    expect(response.ok).toBe(true);
    expect(bindings).toBe(2);
    expect(accepted).toBe(1);
  } finally {
    await gateway.shutdown();
    server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});

it('binds and reconnects to a local manual Gateway without a token or owning its lifetime', async () => {
  const requests: string[] = [], configs: unknown[] = [];
  let available = true;
  const server = createServer(async (request, response) => {
    requests.push(request.url!);
    expect(request.headers.authorization).toBeUndefined();
    if (!available) { response.writeHead(503).end(); return; }
    response.setHeader('Content-Type', 'application/json');
    if (request.url === '/api/dev/status') response.end(JSON.stringify({ kind: 'audio-agent-gateway-dev', protocol_version: '7.0.0', local_history_version: 1 }));
    else if (request.url === '/api/dev/configure') {
      let body = ''; for await (const chunk of request) body += chunk;
      configs.push(JSON.parse(body)); response.end('{"ok":true}');
    } else if (request.url === '/api/scene') response.end('{"ok":true}');
    else response.writeHead(404).end();
  });
  const wss = new WebSocketServer({ server });
  wss.on('connection', (ws, request) => {
    expect(request.headers.authorization).toBeUndefined();
    ws.once('message', raw => {
      expect(JSON.parse(raw.toString()).type).toBe('session.hello');
      ws.send('{"type":"voice.ready"}');
    });
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const url = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const env = { PORTAL_DESKTOP_GATEWAY_URL: url };
  const opts = await options(env);
  const connection = parseConnection('https://being.example/fixture/?token=private-being');
  const sessions = new ChatSessions(opts.directory, scene);
  await sessions.change(connection.endpoint, 'create', 'Other scene');
  const other = sessions.current(connection.endpoint).scene_id;
  await sessions.change(connection.endpoint, 'select', scene.scene_id);
  opts.binding = () => ({ connection, scene: sessions.current(connection.endpoint) });
  const voice = new VoiceGateway(opts);
  try {
    await expect(voice.start('dictation')).rejects.toThrow('不支持语音输入');
    expect(requests).toHaveLength(0);
    await voice.start();
    expect(configs).toEqual([{ being_endpoint: 'https://being.example/fixture', being_token: 'private-being',
      being_scene: scene }]);
    const original = [...wss.clients][0];
    for (const [operation, value, id] of [
      ['rename', 'Other renamed', other], ['delete', other, undefined], ['rename', 'Current renamed', undefined],
      ['select', scene.scene_id, undefined],
    ] as const) {
      await sessions.change(connection.endpoint, operation, value, id);
      await voice.syncBinding();
      expect(voice.send({ type: 'input.mute' })).toBe(true);
      expect([...wss.clients]).toEqual([original]);
    }
    await sessions.change(connection.endpoint, 'create', 'New active scene');
    await voice.syncBinding();
    expect(voice.send({ type: 'input.mute' })).toBe(false);
    await vi.waitFor(() => expect(original.readyState).toBe(3));
    await sessions.change(connection.endpoint, 'select', scene.scene_id);
    await voice.shutdown();
    expect(server.listening).toBe(true);
    await voice.start();
    await voice.shutdown();
    expect(configs).toHaveLength(2);

    available = false;
    await expect(voice.start()).rejects.toThrow('无法连接开发 Gateway');
    expect(configs).toHaveLength(2); // No credentials sent after a failed status check.
    const count = requests.length;
    const packaged = new VoiceGateway({ ...opts, packaged: true });
    await expect(packaged.start()).rejects.toThrow('当前 Loom 凭据不能用于语音代理');
    expect(requests).toHaveLength(count); // Packaged clients ignore the development endpoint.
  } finally {
    await voice.shutdown();
    for (const client of wss.clients) client.terminate();
    await new Promise<void>(resolve => wss.close(() => resolve()));
    server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});

it('cancels a pending connection when the Being credentials change before Gateway configuration', async () => {
  let release!: () => void;
  const pending = new Promise<void>(resolve => { release = resolve; });
  let requests = 0;
  const server = createServer(async (_request, response) => {
    requests++;
    await pending;
    response.setHeader('Content-Type', 'application/json');
    response.end('{"kind":"audio-agent-gateway-dev","protocol_version":"7.0.0","local_history_version":1}');
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const opts = await options({ PORTAL_DESKTOP_GATEWAY_URL: `http://127.0.0.1:${(server.address() as { port: number }).port}` });
  const binding = opts.binding()!;
  opts.binding = () => binding;
  const voice = new VoiceGateway(opts);
  try {
    const failed = expect(voice.start()).rejects.toThrow('语音连接已结束');
    await vi.waitFor(() => expect(requests).toBe(1));
    binding.connection = parseConnection('https://being.example/fixture/?token=new-being-token');
    await voice.syncBinding();
    release(); await failed;
    expect(requests).toBe(1);
  } finally {
    release(); await voice.shutdown();
    server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});

it.each(['https://127.0.0.1:3102', 'http://example.org:3102', 'http://127.0.0.1:3102/path',
  'http://user:password@127.0.0.1:3102', 'http://127.0.0.1:3102?token=bad'])('rejects unsafe development address %s', async url => {
  const voice = new VoiceGateway(await options({ PORTAL_DESKTOP_GATEWAY_URL: url }));
  await expect(voice.start()).rejects.toThrow('开发 Gateway 地址');
});
