import catalog from '../../audio-agent-gateway/gateway/resources/tools.json';
import type { VoiceEvent } from './voice';

export interface VoiceWireHandlers {
  open(): void;
  message(text: string): void;
  error(message?: string, retryable?: boolean): void;
  close(code?: number): void;
}
export interface VoiceWireSocket {
  readonly bufferedAmount: number;
  send(text: string): void;
  close(): void;
}
export interface VoiceTool {
  name: string;
  description?: string;
  parameters?: object;
}
export interface VoiceToolCall {
  id: string;
  name: string;
  args: Record<string, unknown>;
  responseId: string;
  turnId: string;
  transcript: string;
  targets?: readonly string[];
}
interface HistoryMessage {
  role: string;
  content: string;
}
export interface RealtimeVoiceOptions {
  /** Return the socket first, then deliver open/message/close in wire order. */
  connect(handlers: VoiceWireHandlers): VoiceWireSocket;
  emit(event: VoiceEvent): void;
  tools?: readonly VoiceTool[];
  runTool?(call: VoiceToolCall): Promise<unknown>;
  dictation?: boolean;
  history?(): readonly HistoryMessage[] | Promise<readonly HistoryMessage[]>;
  instructions?: string;
  context?(): Record<string, unknown>;
  captureTargets?(): readonly string[];
}
export const voiceToolCatalog: readonly VoiceTool[] = catalog;

interface Turn {
  id: string;
  itemId: string;
  text: string;
  final: boolean;
  targets?: readonly string[];
}
interface Announcement {
  text: string;
  taskIds: string[];
  origin: 'text' | 'announcement';
}
interface Reply {
  id: string;
  wireId?: string;
  turn: Turn;
  announcement?: Announcement;
  text: string;
  finalText: boolean;
  suppressed: boolean;
  hasAudio: boolean;
  toolReply?: 'pending' | 'requested';
}
type LocalActivity = { started: boolean; speaking: boolean; barge: boolean; quietFrames: number };

const speechPrefix = 'conversation.item.input_audio_transcription.';
const handshakeTimeout = 35_000;
const reconnectDelay = 500;
declare const PORTAL_VOICE_INSTRUCTIONS: string | undefined;
const defaultPrompt =
  typeof PORTAL_VOICE_INSTRUCTIONS === 'string'
    ? PORTAL_VOICE_INSTRUCTIONS
    : '你是 Being 的实时语音助手。用中文自然交谈。用户提出需要 Being 执行的任务时，使用可用工具委派。' +
      '区分新工作、修正、进度查询和取消。工具未确认受理前不要声称已提交。用户插话只打断播报，不等于取消后台工作。';
const newTurn = (itemId = ''): Turn => ({ id: crypto.randomUUID(), itemId, text: '', final: false });
const stringValue = (value: unknown) => (typeof value === 'string' ? value : '');

function remember(set: Set<string>, value: string) {
  set.add(value);
  if (set.size > 512) set.delete(set.values().next().value!);
}

/** Seeduplex sends little-endian Float32. Playback consumes little-endian signed PCM16. */
function decodeProviderAudio(encoded: string) {
  const raw = atob(encoded);
  if (raw.length % 4) throw new Error('语音服务返回的音频帧无效。');
  const source = new DataView(Uint8Array.from(raw, char => char.charCodeAt(0)).buffer);
  let result = '';
  for (let i = 0; i < raw.length; i += 4) {
    const sample = source.getFloat32(i, true);
    if (!Number.isFinite(sample)) throw new Error('语音服务返回的音频采样无效。');
    const pcm = Math.round(Math.max(-1, Math.min(1, sample)) * (sample < 0 ? 32768 : 32767));
    result += String.fromCharCode(pcm & 255, (pcm >> 8) & 255);
  }
  return btoa(result);
}

/** One call and its provider reconnects. Device capture, credentials, persistence and task execution belong to the host. */
export class RealtimeVoice {
  private socket?: VoiceWireSocket;
  private epoch = 0;
  private running = false;
  private ready = false;
  private joined = false;
  private muted = false;
  private retries = 0;
  private startDeadline = 0;
  private starting?: Promise<void>;
  private resolveStart?: () => void;
  private rejectStart?: (error: Error) => void;
  private timer?: ReturnType<typeof setTimeout>;
  private asrTimer?: ReturnType<typeof setTimeout>;
  private responseTimer?: ReturnType<typeof setTimeout>;
  private awaitingTranscript = false;
  private lastSpeechAt = 0;
  private barged = false;
  private pendingAudio: string[] = [];
  private pendingBytes = 0;
  private turn = newTurn();
  private turns = new Map<string, Turn>();
  private active?: Reply;
  private playing = new Map<string, Reply>();
  private finished = new Set<string>();
  private events = new Set<string>();
  private reportedCalls = new Set<string>();
  private calls = new Map<string, Promise<unknown>>();
  private announcements: Announcement[] = [];
  private requested?: Announcement;
  private waitForTurn = false;
  private requireResponseStart = false;
  private responsePending = false;
  private dictation?: {
    segments: Map<string, Turn>;
    // Silero may notice a pause inside one ASR item, or the beginning of a new item.
    pendingAudio?: 'speech' | 'unknown';
    completion?: Promise<string>;
    finish?: (error?: string) => void;
  };

  constructor(private options: RealtimeVoiceOptions) {}

  start(): Promise<void> {
    if (this.ready) return Promise.resolve();
    if (this.starting) return this.starting;
    const pending = new Promise<void>((resolve, reject) => {
      this.resolveStart = resolve;
      this.rejectStart = reject;
    });
    this.starting = pending;
    if (!this.running) {
      this.running = true;
      this.startDeadline = Date.now() + handshakeTimeout;
      if (this.options.dictation) this.dictation = { segments: new Map() };
      this.options.emit({ type: 'desktop.voice', state: 'connecting' });
      this.open();
    }
    return pending;
  }

  private open() {
    const timeout = this.joined ? handshakeTimeout : this.startDeadline - Date.now();
    if (timeout <= 0) {
      this.lost('语音服务连接超时。');
      return;
    }
    const epoch = ++this.epoch;
    const handlers: VoiceWireHandlers = {
      open: () => {
        if (epoch !== this.epoch) return;
        let instructions = '用户正在口述输入，请保持安静。';
        if (!this.dictation) {
          const { locale, timeZone } = Intl.DateTimeFormat().resolvedOptions();
          const context = JSON.stringify({ locale, time_zone: timeZone, ...this.options.context?.() }).replace(/</g, '\\u003c');
          instructions = (this.options.instructions || defaultPrompt) + '\n<runtime_context>' + context + '</runtime_context>';
        }
        this.write({
          type: 'session.create',
          session: {
            model: '1.2.6.1',
            instructions,
            audio: {
              input: { format: { type: 'pcm', rate: 16000 } },
              output: {
                format: { type: 'pcm', rate: 24000 },
                voice: 'zh_female_vv_jupiter_bigtts',
                speed: 0,
                loudness: 0,
              },
            },
            tools: this.dictation ? [] : (this.options.tools || []).map(tool => ({ ...tool, type: 'function' })),
          },
        });
      },
      message: raw => {
        if (epoch !== this.epoch) return;
        try {
          const event = JSON.parse(raw);
          if (typeof event?.type !== 'string') throw new Error('语音服务返回了无效事件。');
          if (typeof event.event_id === 'string') {
            if (this.events.has(event.event_id)) return;
            remember(this.events, event.event_id);
          }
          this.receive(event);
        } catch {
          this.lost('语音数据格式错误，请重新连接。');
        }
      },
      error: (message, retryable) => {
        if (epoch === this.epoch) this.lost(message || '无法连接语音服务。', retryable);
      },
      close: code => {
        if (epoch !== this.epoch) return;
        if (code === 1008) this.lost('语音服务拒绝连接，请检查 Being 凭据。', false);
        else this.lost('语音连接已断开。', this.joined || [1001, 1006, 1011, 1012].includes(code ?? 0));
      },
    };
    this.timer = setTimeout(() => this.lost('语音服务连接超时。'), timeout);
    try {
      this.socket = this.options.connect(handlers);
    } catch (error) {
      this.lost(error instanceof Error ? error.message : '无法连接语音服务。', false);
    }
  }

  private write(event: VoiceEvent) {
    if (!this.socket) return false;
    try {
      this.socket.send(JSON.stringify({ ...event, event_id: crypto.randomUUID() }));
      return true;
    } catch {
      this.lost('无法向语音服务发送数据。');
      return false;
    }
  }

  private async connected() {
    const epoch = this.epoch;
    if (this.ready) return;
    const history = this.dictation ? undefined : await this.options.history?.();
    if (epoch !== this.epoch || this.ready) return;
    const items: { id: string; type: string; role: string; content: { type: string; text: string }[] }[] = [];
    let user: HistoryMessage | undefined;
    for (const message of history || []) {
      if (message.role === 'user') user = message;
      else if (message.role === 'assistant' && user) {
        if (user.content.trim() && message.content.trim())
          for (const row of [user, message]) {
            items.push({
              id: crypto.randomUUID(),
              type: 'message',
              role: row.role,
              content: [{ type: 'input_text', text: row.content }],
            });
          }
        user = undefined;
      }
    }
    if (items.length) {
      // Restored messages supply context. Only fresh input or an explicit TTS request owns a reply.
      this.waitForTurn = true;
      this.requireResponseStart = true;
      if (!this.write({ type: 'conversation.item.create', items: items.slice(-40) })) return;
    }
    if (!this.write({ type: this.muted ? 'input_audio_mute.commit' : 'input_audio_unmute.commit' })) return;
    clearTimeout(this.timer);
    this.ready = this.joined = true;
    this.retries = 0;
    this.options.emit({ type: 'desktop.voice', state: 'ready' });
    this.options.emit({ type: 'voice.ready' });
    this.resolveStart?.();
    this.resolveStart = this.rejectStart = this.starting = undefined;
    while (this.pendingAudio.length && this.ready) {
      if (!this.write({ type: 'input_audio_buffer.append', audio: this.pendingAudio[0] })) return;
      this.pendingBytes -= this.pendingAudio.shift()!.length;
    }
    this.playNext();
  }

  private dropSocket(reason: 'disconnected' | 'call_ended') {
    this.epoch++;
    clearTimeout(this.timer);
    clearTimeout(this.asrTimer);
    clearTimeout(this.responseTimer);
    const socket = this.socket;
    this.socket = undefined;
    socket?.close();
    if (this.active?.wireId) remember(this.finished, this.active.wireId);
    this.clearPlayback(reason);
    this.active = undefined;
    this.events.clear();
    this.reportedCalls.clear();
    this.turns.clear();
    this.turn = newTurn();
    this.waitForTurn = true;
    this.requireResponseStart = true;
    this.responsePending =
      this.ready =
      this.awaitingTranscript =
      this.barged =
        false;
    this.lastSpeechAt = 0;
  }

  private lost(message: string, retryable = this.joined) {
    this.dictation?.finish?.(message);
    this.dropSocket('disconnected');
    if (!this.running) return;
    const canReconnect = this.joined
      ? !this.dictation
      : this.retries === 0 && Date.now() + reconnectDelay < this.startDeadline;
    if (!retryable || !canReconnect) {
      this.running = this.joined = false;
      this.resetCall();
      this.rejectStart?.(new Error(message));
      this.starting = this.resolveStart = this.rejectStart = undefined;
      this.options.emit({ type: 'desktop.voice', state: 'error', message });
      return;
    }
    if (this.joined) this.options.emit({ type: 'desktop.voice', state: 'connecting' });
    this.timer = setTimeout(() => this.open(), Math.min(8_000, reconnectDelay * 2 ** this.retries++));
  }

  stop() {
    const active = this.running;
    this.dictation?.finish?.('已取消语音输入。');
    this.running = this.joined = false;
    if (this.ready) this.write({ type: 'session.close' });
    this.dropSocket('call_ended');
    this.rejectStart?.(new Error('语音通话已结束。'));
    this.starting = this.rejectStart = this.resolveStart = undefined;
    this.resetCall();
    if (active) this.options.emit({ type: 'desktop.voice', state: 'stopped' });
  }

  private resetCall() {
    this.pendingAudio = [];
    this.pendingBytes = 0;
    this.calls.clear();
    this.finished.clear();
    this.muted = false;
    this.retries = 0;
    this.dictation = undefined;
  }

  finishDictation(): Promise<string> {
    const draft = this.dictation;
    if (!draft || !this.ready) return Promise.reject(new Error('语音输入未连接。'));
    if (draft.completion) return draft.completion;
    draft.completion = new Promise<string>((resolve, reject) => {
      draft.finish = error => {
        clearTimeout(this.asrTimer);
        draft.finish = undefined;
        const text = [...draft.segments.values()].map(turn => turn.text).filter(Boolean).join('\n');
        if (error || !text) reject(new Error(error || '没有识别到文字。'));
        else resolve(text);
      };
    });
    // A commit acknowledgement alone does not contain the last utterance.
    this.asrTimer = setTimeout(() => {
      const waitingForAsr = [...draft.segments.values()].some(turn => !turn.final) ||
        (!draft.segments.size && !!draft.pendingAudio);
      draft.finish?.(waitingForAsr ? '语音转写等待超时，请重新录入。' : undefined);
    }, 8_000);
    if (this.write({ type: 'input_audio_buffer.commit' }) && !draft.pendingAudio &&
        draft.segments.size && [...draft.segments.values()].every(turn => turn.final)) {
      draft.finish?.();
    }
    return draft.completion;
  }

  send(event: VoiceEvent): boolean {
    if (!this.running) return false;
    if (this.dictation && event.type !== 'input_audio_buffer.append') return false;
    switch (event.type) {
      case 'input.mute':
      case 'input.unmute':
        this.muted = event.type === 'input.mute';
        if (this.muted) {
          this.pendingAudio = [];
          this.pendingBytes = 0;
          this.awaitingTranscript = false;
          clearTimeout(this.asrTimer);
        }
        return (
          !this.ready ||
          this.write({ type: this.muted ? 'input_audio_mute.commit' : 'input_audio_unmute.commit' })
        );
      case 'response.cancel':
        this.interrupt(true);
        return true;
      case 'playback.started':
        return true;
      case 'playback.cancelled':
      case 'playback.ended': {
        const id = stringValue(event.responseId);
        const reply = this.playing.get(id);
        this.playing.delete(id);
        if (reply)
          this.options.emit({
            ...event,
            origin: reply.announcement?.origin || 'model',
            turnId: reply.turn.id,
            taskIds: reply.announcement?.taskIds,
          });
        if (reply && event.type === 'playback.ended') this.playNext();
        return true;
      }
      case 'input_audio_buffer.append': {
        if (this.dictation?.completion) return false;
        if (typeof event.audio !== 'string' || event.audio.length > 64 * 1024) return false;
        if (this.muted) return true;
        if (!this.ready) {
          // Retain microphone packets during a provider reconnect. Returning false leaves
          // the current packet in the capture queue for retry, never a duplicate.
          if (!this.joined || this.pendingBytes + event.audio.length > 213_336) return false;
          this.pendingAudio.push(event.audio);
          this.pendingBytes += event.audio.length;
          return true;
        }
        if (this.socket!.bufferedAmount > 64 * 1024) return false;
        const activity = event.localActivity as LocalActivity | undefined;
        if (this.dictation) {
          if (!activity) this.dictation.pendingAudio = 'unknown';
          else if (activity.started) this.dictation.pendingAudio = 'speech';
        } else if (activity) {
          if (activity.speaking) this.lastSpeechAt = Date.now();
          if (!activity.speaking) this.barged = false;
          const playing = !!this.requested || !!this.active || this.playing.size > 0;
          if ((activity.started && !playing) || (activity.barge && playing && !this.barged)) {
            if (this.turn.final) this.turn = newTurn();
            this.turn.targets ??= this.options.captureTargets?.();
            this.watchTranscription();
          }
          if (activity.barge && playing && !this.barged) {
            this.barged = true;
            this.interrupt(true);
          }
        }
        return this.write({ type: event.type, audio: event.audio });
      }
      default:
        return false;
    }
  }

  private watchTranscription() {
    clearTimeout(this.asrTimer);
    this.awaitingTranscript = true;
    this.asrTimer = setTimeout(() => {
      if (!this.awaitingTranscript || !this.ready || this.muted) return;
      if (Date.now() - this.lastSpeechAt < 1_000) {
        this.watchTranscription();
        return;
      }
      if (!this.write({ type: 'input_audio_buffer.commit' })) return;
      this.asrTimer = setTimeout(() => {
        if (Date.now() - this.lastSpeechAt < 1_000) {
          this.watchTranscription();
          return;
        }
        this.options.emit({ type: 'error', message: '语音识别超时，正在重连，请重说刚才的话。' });
        this.lost('语音识别超时。');
      }, 2_000);
    }, 5_000);
  }

  private interrupt(cancelProvider: boolean) {
    const reply = this.active;
    const generating = reply || this.requested || this.responsePending;
    this.clearPlayback('user_interruption');
    if (reply) {
      reply.suppressed = true;
      remember(this.finished, reply.id);
      if (reply.wireId) remember(this.finished, reply.wireId);
      this.active = undefined;
    }
    clearTimeout(this.responseTimer);
    this.waitForTurn = true;
    this.requireResponseStart = true;
    this.responsePending = false;
    if (generating && cancelProvider) this.write({ type: 'response.cancel' });
  }

  private clearPlayback(reason: string) {
    const requested = this.requested;
    const queued = this.announcements;
    this.requested = undefined;
    this.announcements = [];
    if (!this.active && !this.playing.size && !requested && !queued.length) return;
    const interrupted = new Set(this.playing.values());
    if (this.active && !this.active.hasAudio) interrupted.add(this.active);
    this.playing.clear();
    this.options.emit({ type: 'playback.clear', reason });
    for (const reply of interrupted) {
      reply.suppressed = true;
      this.options.emit({
        type: 'playback.cancelled',
        reason,
        responseId: reply.id,
        turnId: reply.turn.id,
        origin: reply.announcement?.origin || 'model',
        taskIds: reply.announcement?.taskIds,
      });
    }
    // A submitted TTS request need not have a provider response ID yet. On a transport
    // loss the host may requeue notifications that never made it to submission.
    const cancelled = reason === 'disconnected' ? [] : queued;
    if (requested) cancelled.unshift(requested);
    for (const item of cancelled) {
      if (item.taskIds.length)
        this.options.emit({ type: 'playback.cancelled', reason, origin: item.origin, taskIds: item.taskIds });
    }
  }

  speak(text: string, taskIds: string[] = [], origin: Announcement['origin'] = 'announcement') {
    if (!this.running || this.dictation || !text.trim()) return;
    this.announcements.push({ text: text.trim(), taskIds, origin });
    this.playNext();
  }

  private playNext() {
    if (
      !this.ready ||
      this.active ||
      this.playing.size ||
      this.requested ||
      this.awaitingTranscript ||
      this.responsePending
    )
      return;
    this.requested = this.announcements.shift();
    if (this.requested) {
      this.waitForTurn = false;
      this.watchResponse(15_000);
      if (this.requested.taskIds.length)
        this.options.emit({ type: 'playback.requested', origin: this.requested.origin, taskIds: this.requested.taskIds });
      this.write({ type: 'speech_text_buffer.commit', text: this.requested.text });
    }
  }

  private watchResponse(timeout: number) {
    clearTimeout(this.responseTimer);
    this.responseTimer = setTimeout(() => {
      // The model may choose silence. Only a requested TTS or a reply already underway can stall.
      if (this.responsePending) {
        this.responsePending = false;
        this.waitForTurn = true;
        this.playNext();
        return;
      }
      this.options.emit({ type: 'error', message: '语音服务回复超时，正在重连。' });
      this.lost('语音服务回复超时。');
    }, timeout);
  }

  private receive(event: VoiceEvent) {
    const kind = event.type;
    if (kind === 'session.created' || kind === 'session.updated') {
      const epoch = this.epoch;
      void this.connected().catch(() => {
        if (epoch === this.epoch) this.lost('无法恢复本场景的语音上下文。');
      });
      return;
    }
    if (kind === 'session.closed') {
      this.lost('语音服务结束了连接。');
      return;
    }
    if (kind === 'error') {
      const detail = event.error as { message?: string; code?: string | number } | undefined;
      const message = stringValue(detail?.message || event.message).slice(0, 500) || '上游语音服务报错。';
      if (/no active response|no response.*cancel/i.test(message)) return;
      const fatal =
        /invalid.*api.*key|authentication|unauthorized|forbidden|\b(?:401|403|429)\b|quota|arrearage|rate[\s_-]*limit|too many requests/i.test(
          (detail?.code ?? '') + ' ' + message,
        );
      if (
        this.dictation || !this.ready ||
        fatal ||
        /no session active|no active session|timeout|idle|inactivity|no response was generated/i.test(message)
      ) {
        this.lost(message, !fatal && this.joined);
      } else this.options.emit({ type: 'error', message });
      return;
    }
    if (kind.startsWith(speechPrefix) || kind === 'input_audio_buffer.speech_started') {
      this.transcription(kind, event);
      return;
    }
    if (this.dictation || !kind.startsWith('response.')) return;
    const response = event.response as { id?: string; usage?: unknown; status?: string } | undefined;
    if (kind === 'response.done' && response?.usage) return;
    const wireId = stringValue(event.response_id || response?.id);
    if (wireId && this.finished.has(wireId)) return;
    if (this.waitForTurn) {
      if (wireId) remember(this.finished, wireId);
      return;
    }
    if (
      ![
        'response.created',
        'response.output_audio.started',
        'response.output_audio.delta',
        'response.output_audio.done',
        'response.output_text.delta',
        'response.output_text.done',
        'response.done',
        'response.canceled',
        'response.function_call_arguments.done',
      ].includes(kind)
    )
      return;
    if (!this.active && ['response.done', 'response.canceled', 'response.output_audio.done'].includes(kind))
      return;
    if (!this.active) {
      // ASR may finish before the last anonymous chunks of the interrupted output arrive.
      // A continuation is playable once a new response ID, start, text delta or tool call owns it.
      if (this.requireResponseStart && !wireId &&
          (kind === 'response.output_audio.delta' || kind === 'response.output_text.done')) return;
      this.requireResponseStart = false;
      this.active = {
        id: wireId || crypto.randomUUID(),
        wireId,
        turn: this.turn,
        text: '',
        finalText: false,
        hasAudio: false,
        suppressed: false,
        announcement: this.requested,
      };
      this.requested = undefined;
      this.responsePending = false;
      this.options.emit({
        type: 'response.started',
        responseId: this.active.id,
        turnId: this.turn.id,
        origin: this.active.announcement?.origin || 'model',
        taskIds: this.active.announcement?.taskIds,
      });
    }
    const reply = this.active;
    if (wireId && reply.wireId && reply.wireId !== wireId) {
      this.lost('语音服务响应顺序异常。');
      return;
    }
    if (wireId) reply.wireId = wireId;
    this.watchResponse(reply.announcement ? 20_000 : 120_000);
    const base = {
      responseId: reply.id,
      turnId: reply.turn.id,
      origin: reply.announcement?.origin || 'model',
      taskIds: reply.announcement?.taskIds,
    };
    switch (kind) {
      case 'response.output_text.delta': {
        const delta = stringValue(event.delta || event.text);
        if (!delta || reply.finalText) break;
        if (reply.toolReply === 'requested') reply.toolReply = undefined;
        reply.text += delta;
        this.options.emit({ type: 'transcript.delta', role: 'assistant', content: delta, ...base });
        break;
      }
      case 'response.output_text.done': {
        if (reply.finalText) break;
        reply.text = stringValue(event.text || event.transcript) || reply.text;
        reply.finalText = true;
        if (reply.text)
          this.options.emit({ type: 'transcript.final', role: 'assistant', content: reply.text, ...base });
        break;
      }
      case 'response.output_audio.delta': {
        const audio = decodeProviderAudio(stringValue(event.delta || event.audio));
        if (!audio) break;
        if (reply.toolReply === 'requested') reply.toolReply = undefined;
        reply.hasAudio = true;
        this.playing.set(reply.id, reply);
        this.options.emit({ type: 'audio.delta', audio, sampleRate: 24000, ...base });
        break;
      }
      case 'response.function_call_arguments.done':
        void this.toolBatch(reply, Array.isArray(event.items) ? event.items : [event]);
        break;
      case 'response.canceled':
        this.interrupt(false);
        break;
      case 'response.output_audio.done':
      case 'response.done':
        if (response?.status === 'cancelled') {
          this.interrupt(false);
          break;
        }
        clearTimeout(this.responseTimer);
        this.active = undefined;
        // A tool response can be followed by a model continuation in this same turn.
        // Ordinary replies are finished even if the provider sends anonymous trailing audio.
        this.waitForTurn = !reply.toolReply;
        remember(this.finished, reply.id);
        if (reply.wireId) remember(this.finished, reply.wireId);
        if (!reply.finalText && (reply.text || reply.announcement?.text)) {
          this.options.emit({
            type: 'transcript.final',
            role: 'assistant',
            content: reply.text || reply.announcement!.text,
            ...base,
          });
        }
        this.options.emit({ type: 'audio.done', ...base });
        this.options.emit({ type: 'response.done', ...base });
        if (!reply.hasAudio && (reply.text || reply.announcement))
          this.options.emit({ type: 'playback.cancelled', reason: 'no_audio', ...base });
        this.playNext();
        break;
    }
  }

  private transcription(kind: string, event: VoiceEvent) {
    const itemId = stringValue(event.item_id);
    let turn = itemId ? this.turns.get(itemId) : this.turn;
    if (!turn || (!itemId && turn.final && kind.endsWith('started'))) {
      turn = !this.turn.itemId && !this.turn.final ? this.turn : newTurn(itemId);
      if (itemId) {
        turn.itemId = itemId;
        this.turns.set(itemId, turn);
        if (this.turns.size > 512) this.turns.delete(this.turns.keys().next().value!);
      }
      this.turn = turn;
    }
    if (turn.final) return;
    const draft = this.dictation;
    if (draft && !draft.segments.has(turn.id)) {
      draft.segments.set(turn.id, turn);
      draft.pendingAudio = undefined;
    }
    if (!draft) turn.targets ??= this.options.captureTargets?.();
    const base = { role: 'user', turnId: turn.id, itemId: itemId || turn.id };
    if (kind.endsWith('started')) {
      if (!draft) this.watchTranscription();
      return;
    }
    const partial = kind.endsWith('.delta') || kind.endsWith('.text');
    if (!partial && !kind.endsWith('.completed') && !kind.endsWith('.failed')) return;
    const transcript = (
      stringValue(event.delta || event.text || event.transcript) + stringValue(event.stash)
    ).trim();
    const text = kind.endsWith('.failed') ? '' : transcript || (partial ? '' : turn.text);
    if (draft) {
      // New text for this item can account for a local onset. A repeated snapshot or a bare
      // completion can still belong to the previous sentence while the next item is in flight.
      if (turn === this.turn && text && text !== turn.text) draft.pendingAudio = undefined;
      turn.text = text;
      turn.final = !partial;
      if (kind.endsWith('.failed')) {
        this.lost('语音转写失败，请重新录入。');
      } else if (turn.final) {
        if (turn === this.turn && draft.pendingAudio === 'unknown') draft.pendingAudio = undefined;
        if (draft.finish && !draft.pendingAudio && [...draft.segments.values()].every(segment => segment.final)) {
          draft.finish();
        }
      }
      return;
    }
    const meaningful = text && !/^[嗯唔呃啊哦额\s。，、！？.!?]+$/u.test(text);
    // ASR has already interrupted the provider. Clear the previous output locally, including
    // a TTS submission that has not received its first response packet.
    if (turn === this.turn && meaningful &&
      (this.requested || this.active?.announcement || (this.active ? this.active.turn !== turn : this.playing.size)))
      this.interrupt(false);
    if (partial) {
      this.watchTranscription();
      turn.text = text;
      this.options.emit({ type: 'transcript.delta', content: text, replace: true, ...base });
    } else {
      turn.final = true;
      turn.text = meaningful ? text : '';
      if (turn === this.turn) {
        clearTimeout(this.asrTimer);
        this.awaitingTranscript = false;
      }
      if (!turn.text) {
        this.options.emit({ type: 'transcript.discard', ...base });
      } else {
        if (turn === this.turn && !this.active) {
          this.responsePending = true;
          this.watchResponse(60_000);
        }
        this.options.emit({ type: 'transcript.final', content: turn.text, ...base });
      }
      if (turn === this.turn && turn.text) this.waitForTurn = false;
      if (!turn.text) this.playNext();
    }
  }

  private async toolBatch(reply: Reply, batch: VoiceEvent[]) {
    const epoch = this.epoch;
    const calls = batch.filter(item => {
      if (typeof item?.call_id !== 'string' || !item.call_id || this.reportedCalls.has(item.call_id))
        return false;
      remember(this.reportedCalls, item.call_id);
      return true;
    });
    if (!calls.length) return;
    reply.toolReply = 'pending';
    const items = await Promise.all(
      calls.map(async item => {
        const id = String(item.call_id);
        let pending = this.calls.get(id);
        if (!pending) {
          pending = this.tool(reply, id, item);
          this.calls.set(id, pending);
          if (this.calls.size > 512) this.calls.delete(this.calls.keys().next().value!);
        }
        const result = await pending;
        return {
          role: 'tool',
          call_id: id,
          content: [{ type: 'input_text', text: JSON.stringify(result ?? {}) }],
        };
      }),
    );
    if (epoch === this.epoch && this.ready && !reply.suppressed && reply.turn === this.turn && !this.waitForTurn) {
      reply.toolReply = 'requested';
      this.write({ type: 'conversation.item.create', items });
    }
  }

  private async tool(reply: Reply, id: string, item: VoiceEvent) {
    const epoch = this.epoch;
    const name = stringValue(item.name);
    const base = {
      type: 'tool.call',
      surface: 'frontend',
      responseId: reply.id,
      turnId: reply.turn.id,
      name,
      callId: id,
    };
    let result: unknown;
    let failed = false;
    try {
      if (!this.options.runTool || !this.options.tools?.some(tool => tool.name === name))
        throw new Error('此语音会话未启用该工具。');
      const args = typeof item.arguments === 'string' ? JSON.parse(item.arguments) : item.arguments || {};
      if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('语音工具参数无效。');
      const mutates =
        [
          'spawn_thinking',
          'update_agent_task',
          'update_being_memory',
          'cancel_agent_task',
          'schedule_reminder',
        ].includes(name) ||
        (name === 'notes' && args.action !== 'lists' && args.action !== 'show');
      if (mutates && (!reply.turn.final || !reply.turn.text))
        throw new Error('语音转写尚未确认，这次还没提交。');
      this.options.emit({ ...base, status: 'running', arguments: args });
      result = await this.options.runTool({
        id,
        name,
        args,
        responseId: reply.id,
        turnId: reply.turn.id,
        transcript: reply.turn.text,
        targets: reply.turn.targets,
      });
      failed = !!(
        result &&
        typeof result === 'object' &&
        (('error' in result && result.error) ||
          ('status' in result && ['error', 'rejected'].includes(String(result.status))))
      );
      if (JSON.stringify(result ?? {}).length > 65536) throw new Error('工具结果过长，请缩小查询范围。');
    } catch (error) {
      failed = true;
      result = {
        error: true,
        status: 'error',
        user_message: error instanceof Error ? error.message.slice(0, 500) : '工具执行失败。',
      };
    }
    if (epoch === this.epoch && !reply.suppressed)
      this.options.emit({ ...base, status: failed ? 'failed' : 'completed', result });
    return result;
  }
}
