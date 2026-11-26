import { createHash, randomUUID } from 'node:crypto';
import WebSocket from 'ws';
import type { ChatScene } from '../../shared/types';
import type { VoiceEvent } from '../../shared/voice';
import { RealtimeVoice, type VoiceWireHandlers } from '../../shared/voice-realtime';
import { VoiceTasks } from '../../shared/voice-tasks';
import { NativeChatGateway } from '../chat/native-gateway';
import type { ArchivedMessage } from '../chat/native-history';
import { upstreamRequest } from '../chat/proxy';
import type { Connection } from '../chat/connection';
import { DevelopmentGateway } from './legacy-gateway';

export interface VoiceBinding {
  connection: Connection;
  scene: ChatScene;
}
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

function openProvider(binding: VoiceBinding, fixture: NodeJS.ProcessEnv | undefined, handlers: VoiceWireHandlers) {
  const being = new URL(binding.connection.endpoint).pathname.slice(1);
  const override = fixture?.PORTAL_DESKTOP_VOICE_PROVIDER_URL;
  if (
    !override &&
    (!/^[a-zA-Z0-9_-]{1,128}$/.test(being) || !/^[a-zA-Z0-9._~-]{16,2048}$/.test(binding.connection.token))
  )
    throw new Error('当前 Loom 凭据不能用于语音代理。');
  if (override && !fixture?.DOUBAO_API_KEY) throw new Error('测试上游需要 DOUBAO_API_KEY。');
  const url = override || 'wss://101.132.86.87/v1/realtime/' + encodeURIComponent(being);
  const socket = new WebSocket(url, {
    headers: override
      ? { 'X-Api-Key': fixture!.DOUBAO_API_KEY! }
      : { Authorization: 'Bearer ' + binding.connection.token },
    handshakeTimeout: override ? 10000 : 35000,
    maxPayload: 4 * 1024 * 1024,
  });
  socket.on('open', handlers.open);
  socket.on('message', (data, binary) => {
    if (binary) socket.close(1003);
    else handlers.message(data.toString());
  });
  socket.on('error', () => handlers.error('无法连接语音服务。'));
  socket.on('close', code => handlers.close(code));
  return {
    get bufferedAmount() {
      return socket.bufferedAmount;
    },
    send(text: string) {
      socket.send(text);
    },
    close() {
      if (socket.readyState === WebSocket.CONNECTING) socket.terminate();
      else socket.close();
    },
  };
}

export class VoiceGateway {
  private legacy?: DevelopmentGateway;
  private chat: NativeChatGateway;
  private voice?: RealtimeVoice;
  private voiceScene = '';
  private tasks = new Map<string, VoiceTasks>();
  private account = '';
  private generation = 0;
  private get identity() {
    const binding = this.options.binding();
    return binding ? JSON.stringify([binding.connection.endpoint, binding.connection.token]) : '';
  }
  constructor(private options: GatewayOptions) {
    if (!options.packaged && (options.env || process.env).PORTAL_DESKTOP_GATEWAY_URL)
      this.legacy = new DevelopmentGateway(options);
    this.chat = new NativeChatGateway({
      ...options,
      fetcher: fetch,
      onSend: scene => {
        if (scene === this.voiceScene) this.voice?.send({ type: 'response.cancel' });
      },
      onReply: (scene, text) => {
        if (scene === this.voiceScene) this.voice?.speak(text, [], 'text');
      },
      childStates: scene =>
        scene
          ? (this.tasks.get(scene)?.childStates() ?? {})
          : Object.assign({}, ...Array.from(this.tasks.values(), tasks => tasks.childStates())),
      onChildResult: (scene, ids, stream, text, children) => {
        const candidates = scene ? [this.tasks.get(scene)] : this.tasks.values();
        for (const tasks of candidates) if (tasks?.childResult(scene, ids, stream, text, children)) return true;
        return false;
      },
    });
  }
  async ensure() {
    if (this.legacy) return this.legacy.ensure();
    await this.syncBinding();
    this.chat.ensure();
  }
  async syncBinding() {
    if (this.legacy) return this.legacy.syncBinding();
    const key = this.identity;
    if (this.account && this.account !== key) await this.shutdown();
    else if (this.voiceScene && this.voiceScene !== this.options.binding()?.scene.scene_id) this.disconnect();
    if (key === this.account) return;
    this.account = key;
    const binding = this.options.binding();
    if (binding) {
      const scope = this.taskScope(binding);
      for (const scene of this.options.scenes?.() ?? [binding.scene]) {
        if (this.chat.readVoiceTasks(binding.connection.endpoint, scope + scene.scene_id))
          this.sceneTasks({ ...binding, scene });
      }
    }
  }
  async fetchChat(input: RequestInfo | URL, init?: RequestInit) {
    if (this.legacy) return this.legacy.fetchChat(input, init);
    await this.syncBinding();
    return this.chat.fetchChat(input, init);
  }
  async request(pathname: string, init: RequestInit = {}) {
    if (this.legacy) return this.legacy.request(pathname, init);
    await this.syncBinding();
    return this.chat.localHistory(pathname, init);
  }
  private taskScope(binding: VoiceBinding) {
    return createHash('sha256').update(binding.connection.token).digest('hex') + ':';
  }
  private sceneTasks(binding: VoiceBinding) {
    const { connection, scene } = binding,
      id = scene.scene_id;
    const existing = this.tasks.get(id);
    if (existing) return existing;
    const storeKey = this.taskScope(binding) + id;
    const tasks = new VoiceTasks({
      callbackHost: true,
      textWork: () => this.chat.activeText(id),
      stopText: (streamId, order) => this.chat.stopText(id, streamId, order),
      scene: () => (this.options.binding()?.scene.scene_id === id ? this.options.binding()!.scene : scene),
      history: () => this.chat.recentVoiceContext(connection.endpoint, id),
      load: () => this.chat.readVoiceTasks(connection.endpoint, storeKey),
      save: rows => this.chat.saveVoiceTasks(connection.endpoint, storeKey, rows),
      read: key => this.chat.readVoiceTasks(connection.endpoint, JSON.stringify([storeKey, key])),
      write: (key, value) => this.chat.saveVoiceTasks(connection.endpoint, JSON.stringify([storeKey, key]), value),
      request: (route, init) => {
        const request = new Request('beings://chat' + route, init);
        const upstream = upstreamRequest(request, connection);
        return fetch(upstream.url, { ...init, headers: upstream.headers, redirect: 'error', credentials: 'omit' });
      },
      emit: event => {
        if (this.voiceScene === id) this.options.emit(event);
      },
      speak: (text, ids) => {
        if (this.voiceScene === id) this.voice?.speak(text, ids);
      },
      onTask: task => {
        if (task.status !== 'completed' || !task.result) return;
        this.chat.recordVoice(
          'task-result:' + task.id,
          {
            role: 'assistant',
            content: task.result,
            ...scene,
            scene_label: scene.scene_meta?.scene_label,
            source: 'agent-result',
            taskId: task.id,
            voiceKey: 'agent:' + task.id,
            at: new Date(task.completedAt ?? task.createdAt).toISOString(),
            voicePending: task.notificationStatus === 'queued' || task.notificationStatus === 'pending',
          },
          connection.endpoint,
        );
      },
    });
    this.tasks.set(id, tasks);
    return tasks;
  }
  async start() {
    if (this.legacy) return this.legacy.start();
    const generation = this.generation;
    await this.ensure();
    if (generation !== this.generation) throw new Error('语音通话已结束。');
    const binding = this.options.binding();
    if (!binding) throw new Error('请先连接 Being 并选择聊天场景。');
    if (this.voice) return this.voice.start();
    const { scene, connection } = binding;
    const tasks = this.sceneTasks(binding),
      sessionId = randomUUID();
    const pending = new Map<string, ArchivedMessage>();
    this.voiceScene = scene.scene_id;
    const voice = new RealtimeVoice({
      connect: handlers =>
        openProvider(binding, this.options.packaged ? undefined : this.options.env || process.env, handlers),
      history: () => this.chat.recentVoiceContext(connection.endpoint, scene.scene_id),
      context: () => ({ being_name: this.options.townName?.() || connection.being, scene }),
      captureTargets: () => tasks.cancellableIds(),
      tools: tasks.tools,
      runTool: call => tasks.run(call),
      emit: event => {
        if (this.voice !== voice) return;
        if (event.type === 'voice.ready') tasks.resume();
        if (event.type === 'desktop.voice' && (event.state === 'connecting' || event.state === 'error')) tasks.suspend();
        if (event.type === 'playback.requested') tasks.playback(event);
        if (event.type === 'playback.ended' || event.type === 'playback.cancelled') {
          tasks.playback(event);
          const key = 'voice:' + event.responseId,
            message = pending.get(key);
          if (message) {
            this.chat.recordVoice(
              'voice:' + sessionId + ':' + key,
              {
                ...message,
                voicePending: false,
                playbackStatus: event.type === 'playback.ended' ? 'ended' : 'cancelled',
              },
              connection.endpoint,
            );
            pending.delete(key);
          }
        }
        if (
          event.type === 'transcript.final' &&
          typeof event.content === 'string' &&
          event.content &&
          (event.role === 'user' ||
            (event.role === 'assistant' && event.origin !== 'announcement' && event.origin !== 'text'))
        ) {
          const assistant = event.role === 'assistant';
          const key = (assistant ? 'voice:' : 'user:') + (assistant ? event.responseId : event.turnId);
          const row: ArchivedMessage = {
            id: key,
            role: event.role,
            content: event.content,
            ...scene,
            scene_label: scene.scene_meta?.scene_label,
            createdAt: Date.now(),
            sessionId,
            turnId: event.turnId,
            source: assistant ? 'voice-assistant' : 'voice-user',
            at: new Date().toISOString(),
            ...(assistant && { voiceKey: key, voicePending: true, playbackStatus: 'not_started' }),
          };
          if (assistant) pending.set(key, row);
          this.chat.recordVoice('voice:' + sessionId + ':' + key, row, connection.endpoint);
        }
        this.options.emit(event);
      },
    });
    this.voice = voice;
    try {
      await voice.start();
    } catch (error) {
      if (this.voice === voice) this.disconnect();
      throw error;
    }
  }
  send(event: VoiceEvent) {
    if (this.legacy) return this.legacy.send(event);
    if (this.account !== this.identity || this.voiceScene !== this.options.binding()?.scene.scene_id) return false;
    return this.voice?.send(event) ?? false;
  }
  disconnect() {
    if (this.legacy) return this.legacy.disconnect();
    this.generation++;
    this.tasks.get(this.voiceScene)?.suspend();
    this.voice?.stop();
    this.voice = undefined;
    this.voiceScene = '';
  }
  async shutdown() {
    if (this.legacy) return this.legacy.shutdown();
    this.disconnect();
    for (const tasks of this.tasks.values()) tasks.dispose();
    this.tasks.clear();
    this.chat.dispose();
    this.account = '';
  }
}
