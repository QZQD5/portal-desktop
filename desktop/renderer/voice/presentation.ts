import { isVoiceWork, type VoiceEvent, type VoicePresentation, type VoiceWork } from '../../shared/voice';

interface Speech {
  key: string;
  text: string;
  at: number;
  responseId?: string;
  shown: number;
  finished: boolean;
  audioDone: boolean;
  deferred: boolean;
  audio: { at: number; duration: number }[];
}

/** Source deltas feed the current bubble; the speaker clock releases its buffered text. */
export class VoicePresentationStream {
  private messages = new Map<string, Speech>();
  private responses = new Map<string, string>();
  private current?: string;
  private since = Date.now();
  private last = '';
  private tasks = new Map<string, VoiceWork>();
  constructor(private sceneId: string, private publish: (value: VoicePresentation) => void) {}

  private message(key: string) {
    let row = this.messages.get(key);
    if (!row) {
      row = { key, text: '', at: Date.now(), shown: 0, finished: false, audioDone: false,
        deferred: !!this.current, audio: [] };
      this.messages.set(key, row);
      if (!this.current) this.current = key;
    }
    return row;
  }

  receive(event: VoiceEvent) {
    // Voice is bound to the current desktop device. Do not discard events
    // merely because a task carries a stale or unmarked scene id.
    if (!this.sceneId && typeof event.scene_id === 'string' && event.scene_id) this.sceneId = event.scene_id;
    const id = String(event.responseId || '');
    if (event.type === 'response.started' && event.origin !== 'announcement' && event.origin !== 'text') {
      const waiting = this.messages.get(this.current || '');
      this.responses.set(id, 'voice:' + id);
      this.message('voice:' + id).responseId = id;
      if (waiting && !waiting.responseId && !waiting.shown) {
        waiting.deferred = true;
        this.current = 'voice:' + id;
      }
    }
    if (event.type === 'tool.call' && typeof event.callId === 'string') {
      // Delegation tools get their durable activity from task.snapshot/task.activity.
      // Keep a transient run only for ordinary voice tools so a delegated task
      // does not render twice when its task record arrives.
      const toolName = String(event.name || '');
      if (['spawn_thinking', 'update_agent_task', 'schedule_reminder'].includes(toolName)) {
        // Continue processing later events; the task stream owns this UI row.
      } else {
      const key = 'tool:' + event.callId;
      const previous = this.tasks.get(key);
      const status = String(event.status || 'running');
      const terminal = status === 'completed' || status === 'failed';
      const args = event.arguments;
      const result = event.result;
      const entry = {
        type: 'tool' as const, id: event.callId, toolCallId: event.callId,
        name: String(event.name || '工具调用').slice(0, 128),
        inputDetail: (typeof args === 'string' ? args : args ? JSON.stringify(args) : previous?.entries[0]?.inputDetail || '').slice(0, 1500),
        ...(result !== undefined ? { result: (typeof result === 'string' ? result : JSON.stringify(result)).slice(0, 120), resultDetail: (typeof result === 'string' ? result : JSON.stringify(result)).slice(0, 2000) } : {}),
        done: terminal, error: status === 'failed', ts: Number(event.occurred_at || Date.now()),
      };
      const work: VoiceWork = {
        id: key, responseId: typeof event.responseId === 'string' ? event.responseId : previous?.responseId,
        sceneId: this.sceneId, sceneLabel: '', status: terminal ? status : 'running',
        objective: String(event.name || '工具调用').slice(0, 6000), error: status === 'failed' ? String(result || '').slice(0, 6000) : '',
        startedAt: previous?.startedAt || Number(event.occurred_at || Date.now()),
        ...(terminal ? { completedAt: Number(event.occurred_at || Date.now()) } : {}), entries: [entry],
      };
      this.tasks.set(key, work);
      }
    }
    if (event.type === 'task.output.delta' && typeof event.taskId === 'string' && typeof event.content === 'string') {
      if (this.tasks.get(event.taskId)?.outputDiscarded) return;
      const row = this.message('agent:' + event.taskId);
      // Recovery replays cumulative snapshots from the same stream; do not rewind visible text.
      if (!row.text.startsWith(event.content)) row.text = event.content;
    }
    if ((event.type === 'transcript.delta' || event.type === 'transcript.final') && event.role === 'assistant' &&
        event.origin !== 'text' && typeof event.content === 'string') {
      const task = ['announcement', 'agent'].includes(String(event.origin)) && Array.isArray(event.taskIds) ? event.taskIds[0] : undefined;
      if (task && this.tasks.get(task)?.outputDiscarded) return;
      const key = task ? 'agent:' + task : id ? 'voice:' + id : '';
      if (key) {
        const row = this.message(key);
        row.text = event.type === 'transcript.final' || event.replace === true ? event.content : row.text + event.content;
        if (id) {
          // A delayed announcement must not restart typing in an already released bubble.
          if (row.responseId !== id) { row.audio = []; row.audioDone = false; }
          row.responseId = id; this.responses.set(id, key);
        }
        if (event.origin === 'agent') this.complete(row);
      }
    }
    const tasks = event.type === 'task.snapshot' ? event.tasks : event.task ? [event.task] : [];
    if (Array.isArray(tasks)) for (const task of tasks) {
      if (!task || typeof task.id !== 'string') continue;
      if (!this.sceneId && typeof task.scene?.scene_id === 'string' && task.scene.scene_id)
        this.sceneId = task.scene.scene_id;
      if (typeof task.status === 'string' && (Array.isArray(task.activity) || typeof task.objective === 'string')) {
        const previous = this.tasks.get(task.id);
        const entries = (Array.isArray(task.activity) ? task.activity : previous?.entries || [])
          .filter((entry: any) => entry && ['think', 'tool', 'tool-result'].includes(entry.type))
          .slice(-48)
          .map((entry: any) => ({
            type: entry.type, id: typeof entry.id === 'string' ? entry.id.slice(0, 256) : undefined,
            toolCallId: typeof entry.toolCallId === 'string' ? entry.toolCallId.slice(0, 256) : undefined,
            name: typeof entry.name === 'string' ? entry.name.slice(0, 128) : undefined,
            text: typeof entry.text === 'string' ? entry.text.slice(0, 2000) : undefined,
            preview: typeof entry.preview === 'string' ? entry.preview.slice(0, 100) : undefined,
            inputDetail: typeof entry.inputDetail === 'string' ? entry.inputDetail.slice(0, 1500) : undefined,
            result: typeof entry.result === 'string' ? entry.result.slice(0, 120) : undefined,
            resultDetail: typeof entry.resultDetail === 'string' ? entry.resultDetail.slice(0, 2000) : undefined,
            done: typeof entry.done === 'boolean' ? entry.done : undefined,
            error: typeof entry.error === 'boolean' ? entry.error : undefined,
            ts: Number.isFinite(entry.ts) ? entry.ts : undefined,
          }));
        const work = { id: task.id, sceneId: this.sceneId,
          ...(typeof task.chatStreamId === 'string' ? { chatStreamId: task.chatStreamId.slice(0, 256) } : {}),
          sceneLabel: String(task.scene?.scene_meta?.scene_label || '').slice(0, 128), status: task.status,
          objective: String(task.objective || '').slice(0, 6000), error: String(task.error || '').slice(0, 6000),
          startedAt: Number(task.startedAt || task.createdAt || Date.now()),
          ...(Number.isFinite(task.completedAt) ? { completedAt: Number(task.completedAt) } : {}),
          ...(task.discard_output === true ? { outputDiscarded: true } : {}),
          entries };
        if (isVoiceWork(work)) this.tasks.set(task.id, work);
        while (this.tasks.size > 32) {
          const oldest = [...this.tasks.values()].find(row => ['completed', 'failed', 'cancelled'].includes(row.status)) || this.tasks.values().next().value;
          if (!oldest) break;
          this.tasks.delete(oldest.id);
        }
      }
      const key = 'agent:' + task.id;
      if (task.discard_output === true) {
        const row = this.messages.get(key);
        if (row?.responseId) this.responses.delete(row.responseId);
        this.messages.delete(key);
        if (this.current === key) this.current = undefined;
        continue;
      }
      // Text tasks already have a bubble owned by the chat stream.
      if ((task.notificationStatus === 'queued' || task.notificationStatus === 'pending') && !task.chatStreamId) this.message(key);
      if (task.notificationStatus === 'delivered' || task.notificationStatus === 'interrupted' || task.status === 'cancelled') {
        const row = this.messages.get(key);
        if (row) this.complete(row);
      }
    }
    if (event.type === 'audio.done') {
      const row = this.messages.get(this.responses.get(id) || '');
      if (row) {
        row.audioDone = true;
        if (!row.audio.length) this.complete(row);
      }
    }
  }

  audio(id: string, at: number, duration: number) {
    const row = this.messages.get(this.responses.get(id) || '');
    if (row && !row.finished) row.audio.push({ at, duration });
  }

  private complete(row: Speech) {
    row.finished = true;
    row.shown = Array.from(row.text).length;
    if (this.current === row.key) this.current = undefined;
  }

  ended(id: string) {
    const row = this.messages.get(this.responses.get(id) || '');
    if (row) this.complete(row);
  }

  clear() {
    for (const row of this.messages.values()) this.complete(row);
  }

  tick(now: number) {
    this.current ||= [...this.messages.values()].find(row => !row.finished)?.key;
    const rows: VoicePresentation['messages'] = [];
    for (const row of this.messages.values()) {
      const chars = Array.from(row.text);
      if (row.finished) row.shown = chars.length;
      else if (row.key === this.current) {
        if (!row.responseId && !row.deferred) {
          // Being is still generating: expose its actual deltas without waiting for task completion.
          row.shown = chars.length;
        } else {
          const heard = row.audio.reduce((sum, part) => sum + Math.max(0, Math.min(part.duration, now - part.at)), 0);
          const duration = row.audio.reduce((sum, part) => sum + part.duration, 0);
          // shortcut: estimate a 350 ms lead from PCM duration until the provider exposes word timestamps.
          const count = row.audioDone && duration ? Math.ceil(chars.length * (heard + 0.35) / duration)
            : Math.ceil((heard + 0.35) * 8);
          row.shown = Math.max(row.shown, Math.min(chars.length, count));
        }
      }
      rows.push({ key: row.key, text: chars.slice(0, row.shown).join(''), streaming: !row.finished,
        at: row.at, visible: row.finished || row.key === this.current });
    }
    while (rows.length > 256) {
      const index = rows.findIndex(row => !row.streaming);
      if (index < 0) break;
      this.messages.delete(rows[index].key); rows.splice(index, 1);
    }
    const snapshot = { since: this.since, sceneId: this.sceneId, messages: rows, tasks: [...this.tasks.values()] };
    const encoded = JSON.stringify(snapshot);
    if (encoded !== this.last) { this.last = encoded; this.publish(snapshot); }
  }
}
