import type { ChatItem, Message, Run } from './chat';
import type { VoiceState } from '../../../shared/voice';

/** Keep the durable history intact while projecting the live speaker's text onto its bubble. */
export function voiceMessages(items: ChatItem[], voice: VoiceState | null): ChatItem[] {
  const presentation = voice?.presentation;
  const discarded = new Set<string>();
  for (const task of presentation?.tasks || [])
    if (task.outputDiscarded && task.id) discarded.add(task.id);
  // Local history identifies durable task results with an agent:<task id> voice key.
  if (discarded.size) items = items.filter(item => {
    if (item.kind !== 'message' || item.role !== 'being') return true;
    return !item.voiceKey?.startsWith('agent:') || !discarded.has(item.voiceKey.slice(6));
  });
  if (!presentation) return items;
  items = voiceWorkRuns(items, presentation, voice!.state === 'ready');
  const pending = new Map(presentation.messages
    .filter(row => !discarded.has(row.key.replace(/^agent:/, ''))).map(row => [row.key, row]));
  const saved = new Map<string, Message>();
  const result: ChatItem[] = [];
  for (const item of items) {
    if (item.kind !== 'message' || !item.voiceKey || item.sceneId !== presentation.sceneId) { result.push(item); continue; }
    const live = pending.get(item.voiceKey);
    if (live) {
      saved.set(item.voiceKey, item);
    } else if (!(item.voicePending && (item.createdAt || 0) >= presentation.since)) result.push(item);
  }
  for (const live of pending.values()) {
    const archived = saved.get(live.key);
    if (!live.visible || (!live.text && (live.streaming || !archived))) continue;
    const at = archived?.createdAt ?? live.at;
    const message: Message = { kind: 'message', id: 'voice-live:' + live.key, role: 'being',
      timestamp: '', label: 'Being', consecutive: false, sceneId: presentation.sceneId, voiceKey: live.key,
      ...archived, text: !live.streaming && archived ? archived.text : live.text,
      streaming: live.streaming, createdAt: at };
    message.id = 'voice-live:' + live.key;
    const next = result.findIndex(item => item.kind === 'message' && (item.createdAt || 0) > at);
    result.splice(next < 0 ? result.length : next, 0, message);
  }
  // Project each finalized user utterance into the same chronological timeline.
  // This is intentionally separate from the short bottom voice status bar.
  const userRows = voice.userTranscripts || (voice.userTranscript ? [{ id: 'voice-user-live', text: voice.userTranscript, streaming: true, at: Date.now() }] : []);
  for (const row of userRows) {
    const text = row.text.trim();
    if (!text) continue;
    const exists = result.some(item => item.kind === 'message' && item.role === 'user' &&
      item.sceneId === presentation.sceneId && item.text.trim() === text);
    if (exists) continue;
    const message: Message = { kind: 'message', id: 'voice-user-live:' + row.id, role: 'user',
      timestamp: '', label: '你', consecutive: false, sceneId: presentation.sceneId, text,
      streaming: row.streaming, createdAt: row.at };
    const next = result.findIndex(item => (item.kind === 'message' ? item.createdAt || 0 : item.kind === 'run' ? item.start : 0) > row.at);
    result.splice(next < 0 ? result.length : next, 0, message);
  }
  return result;
}

/** Reuse the text-chat execution panel; activity is independent of speech timing. */
function voiceWorkRuns(items: ChatItem[], presentation: NonNullable<VoiceState['presentation']>, connected: boolean): ChatItem[] {
  if (!presentation.tasks?.length) return items;
  const result = items.filter(item => !presentation.tasks!.some(task => item.id === 'voice-work:' + task.id));
  for (const task of presentation.tasks) {
    if (task.sceneId !== presentation.sceneId) continue;
    let ownerIndex = -1;
    if (task.chatStreamId) for (let index = result.length - 1; index >= 0; index--) {
      const item = result[index];
      if (item.kind === 'run' && item.sceneId === task.sceneId && item.streamId === task.chatStreamId) {
        ownerIndex = index;
        break;
      }
    }
    if (ownerIndex < 0 && task.responseId) {
      const responseKey = 'voice:' + task.responseId;
      const messageIndex = result.findIndex(item => item.kind === 'message' && item.sceneId === task.sceneId && item.voiceKey === responseKey);
      if (messageIndex >= 0) ownerIndex = messageIndex;
    }
    const terminal = ['completed', 'failed', 'cancelled'].includes(task.status);
    const pending = task.entries.some(entry => entry.type === 'tool' && !entry.done);
    const owner = result[ownerIndex];
    if (owner?.kind === 'message' && task.responseId) {
      const run: Run = { kind: 'run', id: 'voice-work:' + task.id, sceneId: task.sceneId, sceneLabel: task.sceneLabel,
        entries: task.entries, start: task.startedAt, end: terminal ? task.completedAt || task.startedAt : undefined,
        status: terminal ? task.status === 'failed' ? 'error' : task.status === 'cancelled' ? 'stopped' : 'done'
          : !connected ? 'reconnecting' : pending ? 'working' : 'thinking',
        outcome: terminal ? task.status === 'failed' ? 'error' : task.status === 'cancelled' ? 'stopped' : 'done' : undefined,
        label: task.objective || '工具调用', hint: task.error || task.objective, arg: '', context: pending ? '等待工具返回' : undefined };
      result.splice(ownerIndex, 0, run);
      continue;
    }
    if (owner?.kind === 'run') {
      const stopped = task.status === 'cancelled' ? '任务已停止' : task.outputDiscarded ? '已停止接收回复' : undefined;
      result[ownerIndex] = { ...owner, entries: task.entries.length ? task.entries : owner.entries,
        ...(stopped ? { status: 'stopped', outcome: 'stopped', label: stopped, waitingForReply: false,
          end: owner.end ?? task.completedAt ?? task.startedAt } : {}) };
      continue;
    }
    const run: Run = { kind: 'run', id: 'voice-work:' + task.id, sceneId: task.sceneId, sceneLabel: task.sceneLabel,
      entries: task.entries, start: task.startedAt, end: terminal ? task.completedAt || task.startedAt : undefined,
      status: terminal ? task.status === 'failed' ? 'error' : task.status === 'cancelled' ? 'stopped' : 'done'
        : !connected ? 'reconnecting' : task.status === 'queued' || task.status === 'scheduled' ? 'queued' : pending ? 'working' : 'thinking',
      outcome: terminal ? task.status === 'failed' ? 'error' : task.status === 'cancelled' ? 'stopped' : 'done' : undefined,
      label: terminal ? task.status === 'failed' ? '任务未完成' : task.status === 'cancelled' ? '任务已停止' : '任务已完成'
        : !connected ? '任务 · 状态待刷新' : pending ? '任务 · 等待工具结果' : task.status === 'queued' ? '任务排队中' : '任务处理中',
      hint: task.error || task.objective, arg: '', context: !connected ? '重新连接语音后刷新状态' : pending ? '尚未确认工具返回' : '等待进一步回复' };
    const next = result.findIndex(item => (item.kind === 'message' ? item.createdAt || 0 : item.kind === 'run' ? item.start : 0) > run.start);
    result.splice(next < 0 ? result.length : next, 0, run);
  }
  return result;
}
