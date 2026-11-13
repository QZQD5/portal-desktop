import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { access } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import path from 'node:path';
import WebSocket from 'ws';
import type { ChatScene } from '../../shared/types';
import type { VoiceEvent } from '../../shared/voice';
import type { Connection } from '../chat/connection';

export interface VoiceBinding { connection: Connection; scene: ChatScene }
export interface GatewayOptions {
  directory: string;
  resources: string;
  packaged: boolean;
  binding(): VoiceBinding | null;
  scenes?(): ChatScene[];
  townName?(): string;
  emit(event: VoiceEvent): void;
  env?: NodeJS.ProcessEnv;
}
const COMMANDS = new Set(['input_audio_buffer.append', 'input.mute', 'input.unmute', 'response.cancel',
  'playback.started', 'playback.ended', 'playback.cancelled', 'conversation.item.create', 'task.create', 'task.cancel']);

export class VoiceGateway {
  private child?: ChildProcessWithoutNullStreams;
  private socket?: WebSocket;
  private origin = '';
  private token = '';
  private bindingKey = '';
  private starting?: Promise<void>;
  private preparing?: Promise<void>;
  private voiceScene = '';
  private generation = 0;
  private serviceGeneration = 0;
  private ready = false;
  private abortConnect?: () => void;
  private sessionId = randomUUID();
  private instanceId = randomUUID();
  constructor(private options: GatewayOptions) {}

  private currentKey() {
    const binding = this.options.binding();
    return binding ? JSON.stringify([binding.connection.endpoint, binding.connection.token]) : '';
  }
  async syncBinding() {
    if (this.bindingKey && this.bindingKey !== this.currentKey()) await this.shutdown();
    else if (this.voiceScene && this.voiceScene !== this.options.binding()?.scene.scene_id) this.disconnect();
  }
  ensure() {
    if (!this.preparing) this.preparing = this.prepare().finally(() => { this.preparing = undefined; });
    return this.preparing;
  }
  private async prepare() {
    const binding = this.options.binding();
    if (!binding) throw new Error('请先连接 Being。');
    if (this.bindingKey && this.bindingKey !== this.currentKey()) await this.shutdown();
    if (this.origin) return;
    const generation = this.serviceGeneration;
    this.bindingKey = this.currentKey();
    const env = this.options.env || process.env;
    if (!this.options.packaged && env.PORTAL_DESKTOP_GATEWAY_URL) await this.attach(binding, generation, env);
    else await this.launch(binding, generation);
  }
  async request(pathname: string, init: RequestInit = {}) {
    const identity = this.currentKey();
    await this.ensure();
    if (identity !== this.currentKey()) throw new Error('Being 连接已切换。');
    const headers = new Headers(init.headers);
    headers.delete('origin'); headers.delete('cookie'); headers.delete('x-relay-secret');
    headers.delete('x-portal-being-name');
    if (pathname.split('?', 1)[0] === '/api/heart/chat/stream' && init.method?.toUpperCase() === 'POST')
      headers.set('x-portal-being-name', encodeURIComponent(this.options.townName?.() || ''));
    if (this.token) headers.set('Authorization', 'Bearer ' + this.token);
    else headers.delete('authorization');
    try {
      const response = await fetch(this.origin + pathname, { ...init, headers, redirect: 'error' });
      if (!this.child && response.status === 503) {
        const status = await fetch(this.origin + '/api/dev/status', { redirect: 'error', signal: AbortSignal.timeout(5000) });
        const info = status.ok ? await status.json() : null;
        if (info?.kind === 'audio-agent-gateway-dev' && info.configured === false) {
          // An unconfigured dev process has not accepted this request; binding then retrying is safe.
          await response.body?.cancel();
          this.origin = '';
          await this.ensure();
          if (identity !== this.currentKey()) throw new Error('Being 连接已切换。');
          return await fetch(this.origin + pathname, { ...init, headers, redirect: 'error' });
        }
      }
      return response;
    }
    catch (error) {
      if (!init.signal?.aborted && identity === this.currentKey() && !this.child) this.origin = '';
      throw error;
    }
  }
  async fetchChat(input: string | URL | Request, init?: RequestInit): Promise<Response> {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const binding = this.options.binding();
    if (!binding || !url.href.startsWith(binding.connection.endpoint + '/')) throw new Error('Being 连接已切换。');
    const route = url.pathname.slice(new URL(binding.connection.endpoint).pathname.replace(/\/$/, '').length);
    if (!['/api/chat/stream', '/api/history', '/api/stream/active', '/api/stop'].includes(route))
      throw new Error('Unsupported Gateway chat route');
    url.searchParams.delete('token');
    return this.request('/api/heart/' + route.slice(5) + url.search, init);
  }
  start() {
    if (!this.starting) this.starting = this.connect().finally(() => { this.starting = undefined; });
    return this.starting;
  }
  private async connect() {
    const binding = this.options.binding();
    if (!binding?.scene.scene_id) throw new Error('请先连接 Being 并选择聊天场景。');
    if (this.bindingKey && this.bindingKey !== this.currentKey()) await this.shutdown();
    if (this.ready) return;
    const generation = this.generation;
    this.bindingKey = this.currentKey();
    this.options.emit({ type: 'desktop.voice', state: 'connecting' });
    try {
      await this.ensure();
      if (generation !== this.generation) throw new Error('语音连接已结束。');
      const scene = await this.request('/api/scene', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(binding.scene) });
      if (!scene.ok) throw new Error('语音场景切换失败。');
      this.voiceScene = binding.scene.scene_id;
      const socket = new WebSocket(this.origin.replace(/^http/, 'ws') + '/api/realtime?sessionId=' + this.sessionId,
        { headers: this.token ? { Authorization: 'Bearer ' + this.token } : {}, maxPayload: 2 * 1024 * 1024 });
      this.socket = socket;
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => { socket.terminate(); reject(new Error('实时语音连接超时。')); }, 60_000);
        const fail = () => { clearTimeout(timer); reject(new Error('语音连接已断开，请重试。')); };
        this.abortConnect = fail;
        socket.once('error', fail);
        socket.once('close', () => {
          fail();
          if (this.socket === socket) {
            this.socket = undefined; this.ready = false;
            if (!this.child) this.origin = '';
            this.options.emit({ type: 'desktop.voice', state: 'stopped' });
          }
        });
        socket.on('message', bytes => {
          if (generation !== this.generation || this.socket !== socket) return;
          let event: VoiceEvent;
          try { event = JSON.parse(bytes.toString()); } catch { socket.terminate(); return; }
          if (!event || typeof event.type !== 'string') { socket.terminate(); return; }
          if (event.type === 'session.ping') {
            socket.send(JSON.stringify({ type: 'session.pong', event_id: randomUUID(), request_event_id: event.event_id }));
          }
          if (event.type === 'error' && !this.ready) {
            clearTimeout(timer); reject(new Error(String(event.message || '语音服务尚未就绪。')));
          }
          if (event.type === 'voice.connection' && event.state === 'unavailable' && !this.ready) {
            clearTimeout(timer);
            const detail = String(event.message || '');
            const message = /\b(?:401|403)\b|unauthorized|forbidden/i.test(detail)
              ? '语音 Gateway 鉴权失败，请检查当前 Being 链接中的 Loom token。'
              : /Loom token/.test(detail)
                ? '当前 Being 链接缺少 Loom token。'
                : detail && detail.length <= 200
                  ? detail
                  : '语音服务连接失败，请查看 Gateway 日志。';
            reject(new Error(message));
          }
          if (event.type === 'voice.ready') {
            clearTimeout(timer); this.abortConnect = undefined; this.ready = true;
            this.options.emit({ type: 'desktop.voice', state: 'ready' }); resolve();
          }
          this.options.emit(event);
        });
        socket.once('open', () => socket.send(JSON.stringify({ type: 'session.hello', event_id: randomUUID(),
          protocol: { min: '7.0.0', max: '7.0.0' }, client: { type: 'desktop', instance_id: this.instanceId, label: 'Portal Desktop' },
          being_name: this.options.townName?.() || '',
          capabilities: ['input.audio', 'input.text', 'playback.receipts', 'tasks.commands', 'session.heartbeat'],
          locale: 'zh-CN', time_zone: Intl.DateTimeFormat().resolvedOptions().timeZone,
          connection: { voice_enabled: true, input_enabled: true, output_enabled: true, text_only: false },
        })));
      });
      // Rehydrate the task activity after reconnect/restart. Voice messages
      // are archived separately from Gateway task records, so without this
      // snapshot a reopened voice session shows the reply but loses its
      // expandable execution state.
      try {
        const response = await this.request('/api/tasks?active=false&limit=32');
        const data = response.ok ? await response.json() as { tasks?: unknown } : null;
        if (Array.isArray(data?.tasks)) this.options.emit({ type: 'task.snapshot', tasks: data.tasks });
      } catch { /* Task rehydration is best effort; voice remains usable. */ }
    } catch (error) {
      if (generation === this.generation) {
        this.disconnect();
        this.options.emit({ type: 'desktop.voice', state: 'error', message: error instanceof Error ? error.message : '语音启动失败。' });
      }
      throw error;
    }
  }
  private async attach(binding: VoiceBinding, generation: number, env: NodeJS.ProcessEnv) {
    const url = new URL(env.PORTAL_DESKTOP_GATEWAY_URL!);
    if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || !url.port || url.pathname !== '/' ||
        url.username || url.password || url.search || url.hash) throw new Error('开发 Gateway 地址必须是 http://127.0.0.1:端口。');
    const headers = { 'Content-Type': 'application/json' };
    try {
      const status = await fetch(url.origin + '/api/dev/status', { headers, redirect: 'error', signal: AbortSignal.timeout(10_000) });
      if (!status.ok) throw new Error();
      const info = await status.json();
      if (info.kind !== 'audio-agent-gateway-dev' || info.protocol_version !== '7.0.0' || info.local_history_version !== 1) throw new Error();
    } catch { throw new Error('无法连接开发 Gateway。请确认进程已启动、地址和端口正确。'); }
    if (generation !== this.serviceGeneration) throw new Error('语音连接已结束。');
    let configured: Response;
    try {
      configured = await fetch(url.origin + '/api/dev/configure', { method: 'POST', headers, redirect: 'error',
        signal: AbortSignal.timeout(60_000), body: JSON.stringify({ being_endpoint: binding.connection.endpoint,
          being_token: binding.connection.token, being_scene: binding.scene,
          ...(this.options.scenes ? { being_scenes: this.options.scenes() } : {}),
          ...(env.PORTAL_DESKTOP_VOICE_PROVIDER_URL ? { doubao_url: env.PORTAL_DESKTOP_VOICE_PROVIDER_URL } : {}),
        }) });
    } catch { throw new Error('开发 Gateway 配置连接中断或超时，请重试。'); }
    if (configured.status === 409) throw new Error('开发 Gateway 正在使用另一个语音会话，请先结束该会话。');
    if (configured.status === 503) throw new Error('语音服务尚未就绪，请检查 Gateway 配置。');
    if (!configured.ok) throw new Error(`开发 Gateway 配置失败（HTTP ${configured.status}）。`);
    if (generation !== this.serviceGeneration) throw new Error('语音连接已结束。');
    this.origin = url.origin; this.token = ''; this.bindingKey = this.currentKey();
  }
  private async launch(binding: VoiceBinding, generation: number) {
    const env = this.options.env || process.env;
    // Gateway lives inside this repo (audio-agent-gateway/). Packaged builds use
    // resources/audio-gateway from the same sources.
    const embedded = path.resolve(this.options.resources, '..', 'audio-agent-gateway');
    const root = this.options.packaged
      ? path.join(this.options.resources, 'audio-gateway')
      : path.resolve(env.PORTAL_DESKTOP_GATEWAY_ROOT || embedded);
    const binary = path.join(root, process.platform === 'win32' ? 'AudioAgentGateway.exe' : 'AudioAgentGateway');
    let executable = binary;
    let args: string[] = [];
    try {
      await access(binary);
    } catch {
      await access(path.join(root, 'gateway', '__main__.py')).catch(() => {
        throw new Error(this.options.packaged
          ? '语音组件缺失，请使用包含语音组件的 Portal Desktop 版本。'
          : '语音组件缺失。请在仓库内准备 audio-agent-gateway/.venv。');
      });
      executable = env.PORTAL_DESKTOP_GATEWAY_PYTHON
        || path.join(root, '.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
      args = ['-u', '-m', 'gateway'];
    }
    if (generation !== this.serviceGeneration) throw new Error('语音连接已结束。');
    this.token = randomBytes(32).toString('hex');
    this.bindingKey = this.currentKey();
    const scope = createHash('sha256').update(binding.connection.endpoint).digest('hex');
    const childEnv: NodeJS.ProcessEnv = {
      ...env, PYTHONIOENCODING: 'utf-8', PYTHONUTF8: '1',
    };
    // Voice is reached through the Loom-token reverse proxy documented by the
    // Being service. The desktop gateway must not require a provider API key.
    const child = spawn(executable, args, { cwd: root, env: childEnv, shell: false, windowsHide: true, stdio: 'pipe' });
    this.child = child;
    child.stderr.on('data', bytes => {
      const message = bytes.toString().replace(binding.connection.token, '[redacted]').trim();
      if (message) console.error(`[voice-gateway] ${message}`);
    });
    child.stdin.on('error', () => {});
    child.stdin.write(JSON.stringify({ data_dir: path.join(this.options.directory, 'gateway', scope), access_token: this.token,
      being_endpoint: binding.connection.endpoint, being_token: binding.connection.token, being_scene: binding.scene,
      legacy_history: (this.options.scenes?.() || [binding.scene]).map(scene => ({ scene,
        path: path.join(this.options.directory, 'voice', createHash('sha256').update(binding.connection.endpoint + '\n' + scene.scene_id).digest('hex'), 'gateway.sqlite3') })),
      ...(!this.options.packaged && env.PORTAL_DESKTOP_VOICE_PROVIDER_URL ? { doubao_url: env.PORTAL_DESKTOP_VOICE_PROVIDER_URL } : {}),
    }) + '\n');
    await new Promise<void>((resolve, reject) => {
      const lines = createInterface({ input: child.stdout });
      const timer = setTimeout(() => { child.kill(); reject(new Error('Python Gateway 启动超时。')); }, 60_000);
      const fail = () => { clearTimeout(timer); reject(new Error('Python Gateway 启动失败，请检查语音运行环境。')); };
      child.once('error', fail);
      child.once('exit', () => {
        clearTimeout(timer); lines.close(); fail();
        if (this.child === child) {
          this.child = undefined; this.origin = ''; this.disconnect();
          this.options.emit({ type: 'desktop.voice', state: 'error', message: '语音进程已退出，可以重新开启语音。' });
        }
      });
      lines.on('line', line => {
        try {
          const event = JSON.parse(line);
          if (event.type !== 'gateway.ready') return;
          if (event.local_history_version !== 1) {
            clearTimeout(timer); child.stdin.end('\n'); reject(new Error('Gateway 组件版本过旧，请重新构建本地聊天组件。')); return;
          }
          const url = new URL(event.origin);
          if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || !url.port || url.username || url.password) throw new Error('Invalid Gateway address');
          this.origin = url.origin; clearTimeout(timer); resolve();
        } catch { /* Non-control stdout is not forwarded to the renderer. */ }
      });
    });
  }
  send(event: VoiceEvent) {
    if (!event || !COMMANDS.has(event.type)) throw new Error('不支持的语音操作。');
    if (this.bindingKey !== this.currentKey()) { void this.shutdown(); return false; }
    if (!this.ready || this.socket?.readyState !== WebSocket.OPEN) return false;
    const payload = JSON.stringify({ ...event, event_id: randomUUID(),
      ...(event.type === 'input_audio_buffer.append' ? { being_name: this.options.townName?.() || '' } : {}),
    });
    if (Buffer.byteLength(payload) > 64 * 1024) throw new Error('语音消息过大。');
    if (this.socket.bufferedAmount > 64 * 1024) return false;
    this.socket.send(payload); return true;
  }
  disconnect() {
    this.generation++;
    this.abortConnect?.(); this.abortConnect = undefined;
    this.ready = false;
    const socket = this.socket; this.socket = undefined;
    if (socket?.readyState === WebSocket.CONNECTING) socket.terminate();
    else socket?.close();
    this.options.emit({ type: 'desktop.voice', state: 'stopped' });
  }
  async shutdown() {
    this.serviceGeneration++;
    this.generation++;
    this.disconnect();
    const child = this.child; this.child = undefined;
    this.origin = ''; this.bindingKey = ''; this.sessionId = randomUUID();
    if (child && child.exitCode === null) {
      await new Promise<void>(resolve => {
        const timer = setTimeout(() => { child.kill(); resolve(); }, 8_000);
        child.once('exit', () => { clearTimeout(timer); resolve(); });
        child.stdin.end('\n');
      });
    }
  }
}
