export type VoiceEvent = { type: string; [key: string]: unknown };
export const VOICE_ACTIONS = ['start', 'stop', 'mute', 'interrupt'] as const;
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
  start(): Promise<void>;
  stop(): Promise<void>;
  send(event: VoiceEvent): Promise<boolean>;
  onEvent(callback: (event: VoiceEvent) => void): () => void;
}
