export type VoiceEvent = { type: string; [key: string]: unknown };
export function voiceHandshakeFailure(status: number): [message: string, retryable: boolean] {
  if (status === 401 || status === 403)
    return [`语音服务拒绝了当前 Being 凭据（HTTP ${status}），请检查连接设置。`, false];
  if (status === 429) return ['语音服务已限流（HTTP 429），请稍后重试。', false];
  if ([408, 500, 502, 503, 504].includes(status))
    return [`语音网关暂时不可用（HTTP ${status}），请稍后重试。`, true];
  return [`语音连接请求未获接受（HTTP ${status}）。`, false];
}
export const VOICE_ACTIONS = ['start', 'stop', 'mute', 'interrupt', 'dictate', 'finish-dictation'] as const;
export type VoiceAction = typeof VOICE_ACTIONS[number];
export interface VoiceWorkEntry {
  type: 'think' | 'tool' | 'tool-result';
  id?: string;
  toolCallId?: string;
  name?: string;
  text?: string;
  preview?: string;
  inputDetail?: string;
  result?: string;
  resultDetail?: string;
  done?: boolean;
  error?: boolean;
  ts?: number;
}
export interface VoiceWork {
  id: string;
  responseId?: string;
  chatStreamId?: string;
  sceneId: string;
  sceneLabel: string;
  status: string;
  objective: string;
  error: string;
  startedAt: number;
  completedAt?: number;
  outputDiscarded?: boolean;
  entries: VoiceWorkEntry[];
}
export function isVoiceWork(value: unknown): value is VoiceWork {
  if (!value || typeof value !== 'object') return false;
  const work = value as VoiceWork;
  return [work.id, work.sceneId, work.sceneLabel, work.status, work.objective, work.error].every(text => typeof text === 'string' && text.length <= 6000) &&
    (work.chatStreamId === undefined || (typeof work.chatStreamId === 'string' && work.chatStreamId.length <= 256)) &&
    (work.responseId === undefined || (typeof work.responseId === 'string' && work.responseId.length <= 256)) &&
    Number.isFinite(work.startedAt) && (work.completedAt === undefined || Number.isFinite(work.completedAt)) &&
    (work.outputDiscarded === undefined || typeof work.outputDiscarded === 'boolean') &&
    Array.isArray(work.entries) && work.entries.length <= 48 && work.entries.every(entry => entry &&
      ['think', 'tool', 'tool-result'].includes(entry.type) &&
      ['id', 'toolCallId', 'name', 'text', 'preview', 'inputDetail', 'result', 'resultDetail'].every(key => {
        const text = entry[key as keyof VoiceWorkEntry]; return text === undefined || (typeof text === 'string' && text.length <= 6000);
      }) && (entry.ts === undefined || Number.isFinite(entry.ts)) &&
      (entry.done === undefined || typeof entry.done === 'boolean') && (entry.error === undefined || typeof entry.error === 'boolean'));
}
export interface VoicePresentation {
  since: number;
  sceneId: string;
  messages: { key: string; text: string; streaming: boolean; at: number; visible: boolean }[];
  tasks?: VoiceWork[];
}
export interface VoiceState {
  state: 'stopped' | 'connecting' | 'ready' | 'error';
  muted: boolean;
  dictation?: 'recording' | 'finishing';
  dictationAvailable?: boolean;
  microphone?: 'starting' | 'ready' | 'recovering' | 'unavailable';
  inputLevel?: number;
  error: string;
  transcript: string;
  userTranscript?: string;
  userTranscripts?: { id: string; text: string; streaming: boolean; at: number }[];
  task: string;
  presentation?: VoicePresentation;
}
export function isVoiceState(value: unknown): value is VoiceState {
  if (!value || typeof value !== 'object') return false;
  const data = value as VoiceState;
  return ['stopped', 'connecting', 'ready', 'error'].includes(data.state) && typeof data.muted === 'boolean' &&
    (data.dictation === undefined || ['recording', 'finishing'].includes(data.dictation)) &&
    (data.dictationAvailable === undefined || typeof data.dictationAvailable === 'boolean') &&
    (data.microphone === undefined || ['starting', 'ready', 'recovering', 'unavailable'].includes(data.microphone)) &&
    (data.inputLevel === undefined || (Number.isFinite(data.inputLevel) && data.inputLevel >= 0 && data.inputLevel <= 1)) &&
    [data.error, data.transcript, data.task].every(text => typeof text === 'string' && text.length <= 6000) &&
    (data.userTranscript === undefined || (typeof data.userTranscript === 'string' && data.userTranscript.length <= 6000)) &&
    (data.userTranscripts === undefined || (Array.isArray(data.userTranscripts) && data.userTranscripts.length <= 64 && data.userTranscripts.every(row => row && typeof row.id === 'string' && row.id.length <= 256 && typeof row.text === 'string' && row.text.length <= 6000 && typeof row.streaming === 'boolean' && Number.isFinite(row.at)))) &&
    (!data.presentation || (Number.isFinite(data.presentation.since) && typeof data.presentation.sceneId === 'string' &&
      (data.presentation.tasks === undefined || (Array.isArray(data.presentation.tasks) && data.presentation.tasks.length <= 32 && data.presentation.tasks.every(isVoiceWork))) &&
      Array.isArray(data.presentation.messages) && data.presentation.messages.length <= 256 &&
      data.presentation.messages.every(row => row && typeof row.key === 'string' && row.key.length <= 256 &&
        typeof row.text === 'string' && row.text.length <= 1_000_000 && Number.isFinite(row.at) &&
        typeof row.streaming === 'boolean' && typeof row.visible === 'boolean')));
}
export interface VoiceAPI {
  localActivity?: boolean;
  start(mode?: 'dictation'): Promise<void>;
  finishDictation?(): Promise<string>;
  stop(): Promise<void>;
  send(event: VoiceEvent): Promise<boolean>;
  onEvent(callback: (event: VoiceEvent) => void): () => void;
}
