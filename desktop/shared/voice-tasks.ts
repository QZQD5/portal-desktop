import { ChatEventDecoder } from './chat-sse';
import type { ChatScene } from './types';
import type { VoiceEvent, VoiceWorkEntry } from './voice';
import { voiceToolCatalog, type VoiceToolCall } from './voice-realtime';
import {
  nextOccurrence,
  sensitiveVoiceText,
  voiceNotes,
  type Recurrence,
  type VoiceSchedule,
} from './voice-utilities';
import {
  callbackReply,
  cancelChildTasks,
  observeChildTool,
  pendingChildren,
  type ChildReceipt,
} from './subagent-receipts';

type TaskStatus = 'scheduled' | 'queued' | 'running' | 'delegated' | 'completed' | 'cancelled' | 'failed';
interface Task {
  id: string;
  turnId: string;
  scene: ChatScene;
  objective: string;
  message: string;
  status: TaskStatus;
  createdAt: number;
  startedAt?: number;
  completedAt?: number;
  streamId?: string;
  submitted?: boolean;
  notificationStatus?: 'queued' | 'pending' | 'interrupted' | 'delivered';
  result: string;
  error?: string;
  activity: VoiceWorkEntry[];
  schedule?: VoiceSchedule;
  being?: ChildReceipt;
  streamScene?: string | null;
  parentFinished?: boolean;
  callbackStreamId?: string;
}
interface Options {
  scene(): ChatScene;
  request(route: string, init: RequestInit): Promise<Response>;
  emit(event: VoiceEvent): void;
  speak(text: string, ids: string[]): void;
  load?(): unknown;
  save?(tasks: readonly Task[]): void;
  read?(key: string): unknown;
  write?(key: string, value: unknown): void;
  onTask?(task: Task): void;
  history?(): readonly { role: string; content: string }[];
  /** Desktop chat already discovers callback streams. Browsers poll while a child is outstanding. */
  callbackHost?: boolean;
  textWork?(): { stream_id: string; scene_id: string; status: string; order?: number } | undefined;
  stopText?(streamId: string, order?: number): Promise<Response>;
}
const names = new Set([
  'spawn_thinking',
  'update_being_memory',
  'update_agent_task',
  'cancel_agent_task',
  'get_agent_task_status',
  'get_current_time',
]);
const open = (task: Task) => task.status === 'queued' || task.status === 'running';
const pending = (task: Task) => open(task) || task.status === 'scheduled' || task.status === 'delegated';
const fail = (message: string) => ({ status: 'error', message });
const textTarget = (text: { stream_id: string; order?: number }) =>
  'text:' + (text.order ?? '') + ':' + text.stream_id;

/** One Being and scene own the task queue, including work still running after hangup. */
export class VoiceTasks {
  readonly tools;
  private tasks: Task[];
  private queue = Promise.resolve();
  private lifetime = new AbortController();
  private listening = false;
  private timer?: ReturnType<typeof setTimeout>;
  private callbackTimer?: ReturnType<typeof setTimeout>;
  private cancellations = new Map<string, Promise<{ status: string; message?: string; task_id?: string }>>();
  private reading?: { id: string; reader: ReadableStreamDefaultReader<Uint8Array> };
  constructor(private options: Options) {
    this.tools = voiceToolCatalog.filter(
      tool =>
        names.has(tool.name) ||
        (tool.name === 'notes' && options.read && options.write) ||
        (tool.name === 'schedule_reminder' && options.load && options.save),
    );
    const stored = options.load?.();
    this.tasks = Array.isArray(stored)
      ? stored.filter(
          task =>
            task?.scene?.scene_id === options.scene().scene_id &&
            typeof task.id === 'string' &&
            typeof task.message === 'string' &&
            Array.isArray(task.activity),
        )
      : [];
    // Older stores used 'pending' for both unplayed and partly spoken results.
    // Only 'queued' proves this announcement has never been submitted to TTS.
    this.interruptPendingAnnouncements();
    for (const task of this.tasks.filter(open)) this.enqueue(task);
    this.armScheduler();
    this.watchCallbacks();
  }
  private changed(task: Task, type: string) {
    if (this.lifetime.signal.aborted) return;
    this.options.save?.(this.tasks);
    this.options.onTask?.(task);
    if (this.listening) this.options.emit({ type, task: { ...task } });
  }
  resume() {
    if (this.listening) return;
    this.listening = true;
    this.options.emit({ type: 'task.snapshot', tasks: this.tasks });
    for (const task of this.tasks) this.announce(task);
  }
  suspend() {
    this.listening = false;
    this.interruptPendingAnnouncements();
  }
  private interruptPendingAnnouncements() {
    const pending = this.tasks.filter(task => task.notificationStatus === 'pending');
    if (!pending.length) return;
    for (const task of pending) task.notificationStatus = 'interrupted';
    this.options.save?.(this.tasks);
    for (const task of pending) this.options.onTask?.(task);
  }
  dispose() {
    this.suspend();
    clearTimeout(this.timer);
    clearTimeout(this.callbackTimer);
    this.lifetime.abort();
    void this.reading?.reader.cancel().catch(() => {});
  }
  playback(event: VoiceEvent) {
    const ids = event.taskIds;
    if (!Array.isArray(ids)) return;
    let status: NonNullable<Task['notificationStatus']>;
    switch (event.type) {
      case 'playback.requested': status = 'pending'; break;
      case 'playback.ended': status = 'delivered'; break;
      case 'playback.cancelled': status = 'interrupted'; break;
      default: return;
    }
    for (const task of this.tasks) {
      if (!ids.includes(task.id) || (task.notificationStatus !== 'queued' && task.notificationStatus !== 'pending')) continue;
      if (task.notificationStatus === status) continue;
      if (task.notificationStatus === 'queued' && status === 'interrupted' && event.reason === 'disconnected') continue;
      task.notificationStatus = status;
      this.changed(task, 'task.notification.' + status);
    }
  }
  private announce(task: Task) {
    if (!this.listening || task.notificationStatus !== 'queued') return;
    this.options.speak(task.status === 'failed' ? task.error || 'Being 任务执行失败。' : task.result, [
      task.id,
    ]);
  }
  childStates(): Record<string, string> {
    return Object.assign({}, ...this.tasks.map(task => task.being?.subagents || {}));
  }
  cancellableIds() {
    const ids = this.tasks.filter(pending).map(task => task.id);
    const text = this.options.textWork?.();
    if (text?.scene_id === this.options.scene().scene_id && this.options.stopText) ids.push(textTarget(text));
    return ids;
  }
  childResult(
    sceneId: string,
    ids: string[],
    streamId: string,
    text: string,
    children: Record<string, string>,
  ) {
    if (this.lifetime.signal.aborted) return false;
    const owners = this.tasks.filter(
      task =>
        (!sceneId || task.scene.scene_id === sceneId) &&
        ids.some(id => id === task.id || Object.hasOwn(task.being?.subagents || {}, id)),
    );
    if (owners.length !== 1) return false;
    const task = owners[0];
    if (task.status === 'cancelled' || (task.callbackStreamId === streamId && !pending(task))) return true;
    if (!task.being || !pending(task)) return false;
    const known = task.being.subagents || {};
    const updates = Object.entries(children).filter(([id]) => Object.hasOwn(known, id));
    if (!updates.length) return false;
    Object.assign(known, Object.fromEntries(updates));
    if (pendingChildren(task.being)) {
      this.changed(task, 'task.progress');
      return false;
    }
    const states = Object.values(known);
    task.status = states.every(value => value === 'done')
      ? 'completed'
      : states.every(value => value === 'cancelled')
        ? 'cancelled'
        : 'failed';
    task.callbackStreamId = streamId;
    task.completedAt = Date.now();
    task.result = task.status === 'completed' ? text.trim() || 'Being 已完成任务。' : '';
    if (task.status === 'failed') task.error = text.trim() || 'Being 子任务未能完成。';
    if (task.status !== 'cancelled') task.notificationStatus = 'queued';
    this.changed(task, 'task.' + task.status);
    this.announce(task);
    return true;
  }
  private watchCallbacks() {
    if (
      this.options.callbackHost ||
      this.callbackTimer ||
      this.lifetime.signal.aborted ||
      !this.tasks.some(task => task.status === 'delegated')
    )
      return;
    this.callbackTimer = setTimeout(async () => {
      try {
        const active = await this.active(0);
        if (active?.origin !== 'callback' || !active.stream_id || !Array.isArray(active.events)) return;
        let source = active.task_id,
          scene = active.scene_id,
          start = 0;
        for (let i = 0; i < active.events.length; i++) {
          const item = active.events[i];
          if (item.event === 'meta' && 'scene_id' in item.data) scene = item.data.scene_id;
          if (item.event !== 'message_stop' && !(active.finished && i === active.events.length - 1)) continue;
          const reply = callbackReply(active.events.slice(start, i + 1), source, this.childStates());
          const text = reply.items
            .filter(row => row.event === 'content_block_delta')
            .map(row => row.data.delta?.text || '')
            .join('');
          this.childResult(scene || '', reply.ids, active.stream_id, text, reply.children);
          source = reply.sourceId;
          start = i + 1;
        }
      } catch {
        /* A missed poll does not change task state. */
      } finally {
        this.callbackTimer = undefined;
        this.watchCallbacks();
      }
    }, 1_500);
  }
  async run(call: VoiceToolCall): Promise<unknown> {
    if (this.lifetime.signal.aborted) return fail('Being 连接已切换。');
    const { name, args } = call;
    if (name === 'get_current_time')
      return {
        status: 'ok',
        iso_utc: new Date().toISOString(),
        local_time: new Date().toLocaleString(),
        time_zone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      };
    if (name === 'notes' && this.options.read && this.options.write) {
      return voiceNotes({ read: this.options.read, write: this.options.write }, args);
    }
    if (name === 'schedule_reminder' && this.options.load && this.options.save) {
      const reminder = typeof args.reminder === 'string' ? args.reminder.trim() : '';
      const iso = typeof args.execute_at === 'string' ? args.execute_at : '';
      const executeAt = Date.parse(iso);
      if (
        !reminder ||
        !/(?:Z|[+-]\d\d:\d\d)$/i.test(iso) ||
        !Number.isFinite(executeAt) ||
        executeAt <= Date.now()
      )
        return fail('请给出提醒内容和带时区的未来时间。');
      const recurrence = (args.recurrence ?? 'once') as Recurrence;
      const type = args.type ?? 'reminder';
      if (
        !['once', 'daily', 'weekly', 'weekdays'].includes(recurrence) ||
        (type !== 'reminder' && type !== 'task')
      )
        return fail('提醒类型或重复方式无效。');
      const existing = this.tasks.find(
        task =>
          task.turnId === call.turnId &&
          task.objective === reminder &&
          task.schedule?.startAt === executeAt &&
          task.schedule.recurrence === recurrence &&
          task.schedule.type === type,
      );
      if (existing)
        return { status: 'already_submitted', task_id: existing.id, series_id: existing.schedule!.seriesId };
      const schedule: VoiceSchedule = {
        executeAt,
        startAt: executeAt,
        recurrence,
        type,
        seriesId: crypto.randomUUID(),
        timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      };
      const task: Task = {
        id: crypto.randomUUID(),
        scene: this.options.scene(),
        status: 'scheduled',
        schedule,
        message: reminder,
        objective: reminder,
        turnId: call.turnId,
        activity: [],
        result: '',
        createdAt: Date.now(),
      };
      this.tasks.push(task);
      this.changed(task, 'task.scheduled');
      this.armScheduler();
      return {
        status: 'scheduled',
        task_id: task.id,
        series_id: schedule.seriesId,
        execute_at: iso,
        type,
        recurrence,
      };
    }
    const scope = name === 'cancel_agent_task' ? new Set(call.targets ?? this.cancellableIds()) : undefined;
    const text = this.options.textWork?.();
    const textWork =
      text?.scene_id === this.options.scene().scene_id && (!scope || scope.has(textTarget(text)))
        ? text
        : undefined;
    const targets = this.tasks
      .filter(task => !scope || scope.has(task.id))
      .filter(task =>
        args.task_id
          ? task.id === args.task_id
          : args.series_id
            ? task.schedule?.seriesId === args.series_id && pending(task)
            : args.stream_id
              ? task.streamId === args.stream_id && open(task)
              : pending(task),
      );
    if (name === 'get_agent_task_status')
      return {
        active_count: targets.filter(pending).length + Number(!!textWork && !args.task_id),
        textWork: args.task_id ? undefined : textWork,
        queued_count: targets.filter(task => task.status === 'queued').length,
        tasks: targets.slice(-32).map(({ id, objective, result, error, status, schedule }) => ({
          id,
          objective,
          result,
          error,
          status,
          series_id: schedule?.seriesId,
          execute_at: schedule?.executeAt,
        })),
      };
    const objective = typeof args.objective === 'string' ? args.objective.trim() : '';
    if (name === 'spawn_thinking' || name === 'update_being_memory') {
      const prior = this.tasks.find(task => task.turnId === call.turnId && !task.schedule);
      if (prior) return { status: 'already_submitted', task_id: prior.id };
      if (!objective) return fail('请说明任务目标。');
      if (name === 'update_being_memory' && sensitiveVoiceText.test(objective))
        return fail('这类敏感信息不能写入长期记忆。');
      let message = objective;
      if (call.transcript && call.transcript !== objective) message += '\n用户本轮原话：' + call.transcript;
      const context = this.options
        .history?.()
        .slice(-8)
        .map(item => item.role + '：' + item.content)
        .join('\n');
      if (context) message += '\n以下是语音会话背景，仅用于理解本次请求，不要再次执行历史任务：\n' + context;
      if (name === 'update_being_memory')
        message += '\n请由当前 Being 直接处理本次身份或长期记忆写入，按实际结果回复。';
      const task: Task = {
        id: crypto.randomUUID(),
        turnId: call.turnId,
        scene: this.options.scene(),
        objective,
        message,
        status: 'queued',
        result: '',
        activity: [],
        createdAt: Date.now(),
      };
      this.tasks.push(task);
      this.changed(task, 'task.accepted');
      this.enqueue(task);
      return { status: 'accepted', task_id: task.id };
    }
    if (name !== 'update_agent_task' && name !== 'cancel_agent_task') return fail('此工具不可用。');
    if (name === 'cancel_agent_task') {
      if ([args.task_id, args.series_id, args.stream_id, args.all].filter(Boolean).length > 1)
        return fail('请指定一种取消范围。');
      const implicit = !args.task_id && !args.stream_id && !args.series_id && !args.all;
      if (implicit && scope!.size > 1) return fail('本轮有多项工作，请明确要停止哪一项。');
      if (textWork && (args.stream_id === textWork.stream_id || implicit) && this.options.stopText) {
        const result = await this.options.stopText(textWork.stream_id, textWork.order);
        if (!(await confirmedStop(result, textWork.stream_id)))
          return fail('Being 尚未确认停止这条文字请求。');
        return { status: 'cancelled', stream_id: textWork.stream_id };
      }
      if (args.all === true || args.series_id) {
        const results = [];
        for (const task of targets) results.push(await this.run({ ...call, args: { task_id: task.id } }));
        if (args.all === true && textWork)
          results.push(await this.run({ ...call, args: { stream_id: textWork.stream_id } }));
        if (!results.length) return fail('当前场景没有可取消的目标。');
        return {
          status: results.every(result => (result as { status: string }).status === 'cancelled')
            ? 'cancelled'
            : 'partial',
          results,
        };
      }
    }
    if (targets.length !== 1 || !pending(targets[0]))
      return fail('请先查询当前场景的任务，并指定一项进行操作。');
    const task = targets[0];
    if (name === 'cancel_agent_task') {
      const pendingStop = this.cancellations.get(task.id);
      if (pendingStop) return pendingStop;
      const stopped = this.cancel(task).finally(() => this.cancellations.delete(task.id));
      this.cancellations.set(task.id, stopped);
      return stopped;
    }
    if (!objective) return fail('请说明修正内容。');
    if (!task.submitted) {
      task.objective = objective;
      task.message = task.schedule ? objective : task.message + '\n用户最新修正：' + objective;
    } else {
      if (!task.streamId) return fail('尚未取得原任务流标识，请在聊天中核对。');
      const active = await this.active();
      if (
        !active ||
        active.stream_id !== task.streamId ||
        active.finished ||
        (active.scene_id && active.scene_id !== task.scene.scene_id)
      )
        return fail('原任务流已结束或发生变化。');
      const receipt = await this.post('/api/chat/stream', {
        ...task.scene,
        message: '用户修正当前工作的要求：\n' + objective,
      });
      await receipt.body?.cancel();
      if (receipt.status !== 202) return fail('修正请求尚未确认，请在聊天中核对。');
      task.objective = objective;
    }
    this.changed(task, 'task.updated');
    return { status: 'accepted', task_id: task.id };
  }
  private async cancel(task: Task) {
    if (task.submitted && !task.parentFinished) {
      if (!task.streamId) return fail('尚未取得原任务流标识，请在聊天中核对。');
      const active = await this.active();
      if (
        !active ||
        active.stream_id !== task.streamId ||
        active.finished ||
        (active.scene_id && active.scene_id !== task.scene.scene_id)
      )
        return fail('原任务流已结束或发生变化。');
      const receipt = await this.post('/api/stop', { stream_id: task.streamId });
      if (!(await confirmedStop(receipt, task.streamId))) return fail('Being 尚未确认停止这项任务。');
      task.parentFinished = true;
      if (this.reading?.id === task.id) await this.reading.reader.cancel().catch(() => {});
    }
    if (task.being && pendingChildren(task.being)) {
      task.status = 'delegated';
      this.changed(task, 'task.delegated');
      this.watchCallbacks();
      const failure = await cancelChildTasks(task.being, task.scene, (route, body) => this.post(route, body));
      if (failure) {
        await failure.body?.cancel();
        this.changed(task, 'task.progress');
        return fail('子代理停止尚未确认，请在 Being 聊天中核对。');
      }
    }
    task.status = 'cancelled';
    task.completedAt = Date.now();
    task.result = '';
    this.changed(task, 'task.cancelled');
    if (task.schedule) this.armScheduler();
    return { status: 'cancelled', task_id: task.id };
  }
  private armScheduler() {
    clearTimeout(this.timer);
    const next = this.tasks
      .filter(task => task.status === 'scheduled' && task.schedule)
      .sort((a, b) => a.schedule!.executeAt - b.schedule!.executeAt)[0];
    if (!next || this.lifetime.signal.aborted) return;
    const due = next.schedule!;
    this.timer = setTimeout(
      () => {
        const now = Date.now();
        if (now >= due.executeAt) {
          const nextAt = nextOccurrence(due, now);
          const repeat: Task | undefined = nextAt
            ? { ...next, id: crypto.randomUUID(), createdAt: now, schedule: { ...due, executeAt: nextAt } }
            : undefined;
          if (repeat) this.tasks.push(repeat);
          if (due.type === 'reminder') {
            next.result = next.objective;
            next.status = 'completed';
            next.startedAt = next.completedAt = now;
            next.notificationStatus = 'queued';
            this.changed(next, 'task.completed');
            this.announce(next);
          } else {
            next.status = 'queued';
            this.changed(next, 'task.accepted');
            this.enqueue(next);
          }
          if (repeat) this.changed(repeat, 'task.scheduled');
        }
        this.armScheduler();
      },
      Math.min(2_147_483_647, Math.max(0, due.executeAt - Date.now())),
    );
  }
  private async active(after?: number) {
    const response = await this.options.request(
      '/api/stream/active' + (after === undefined ? '' : '?after=' + after),
      { signal: this.lifetime.signal },
    );
    if (!response.ok) throw new Error('读取 Being 任务状态失败（' + response.status + '）。');
    return response.status === 204 ? null : await response.json();
  }
  private post(route: string, body: object) {
    return this.options.request(route, {
      method: 'POST',
      signal: this.lifetime.signal,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }
  private enqueue(task: Task) {
    this.queue = this.queue.then(async () => {
      try {
        if (!open(task) || this.lifetime.signal.aborted) return;
        await this.execute(task);
        await this.cancellations.get(task.id)?.catch(() => {});
        if (!open(task) || this.lifetime.signal.aborted) return;
        task.parentFinished = true;
        if (task.being && pendingChildren(task.being)) {
          task.status = 'delegated';
          task.result = '';
          this.changed(task, 'task.delegated');
          this.watchCallbacks();
          return;
        }
        task.result = task.result.trim();
        if (!task.result) throw new Error('Being 未返回任务结果，请在聊天中核对。');
        task.status = 'completed';
      } catch (error) {
        await this.cancellations.get(task.id)?.catch(() => {});
        if (!open(task) || this.lifetime.signal.aborted) return;
        task.status = 'failed';
        task.error = error instanceof Error ? error.message : 'Being 任务执行失败。';
      }
      task.completedAt = Date.now();
      task.notificationStatus = 'queued';
      this.changed(task, 'task.' + task.status);
      this.announce(task);
    });
  }
  private async execute(task: Task) {
    if (!task.submitted) {
      while (true) {
        const active = await this.active();
        if (!open(task)) return;
        if (!active || active.finished) break;
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      task.status = 'running';
      task.submitted = true;
      task.startedAt = Date.now();
      this.changed(task, 'task.running');
      const message =
        task.message +
        '\n\n如果委派子代理，请在委派说明中保留下方原任务标记。回调回复的首行原样保留标记，下一行写正文。当前直接回复无需该标记。\n<!-- gateway-task: ' +
        task.id +
        ' -->';
      const response = await this.post('/api/chat/stream', { message, ...task.scene });
      if (response.status === 202) throw new Error('Being 已接收任务，请在聊天中核对结果。');
      if (
        !response.ok ||
        !response.body ||
        !response.headers.get('content-type')?.includes('text/event-stream')
      )
        throw new Error('Being 未返回任务流（' + response.status + '）。');
      const reader = response.body.getReader(),
        decoder = new ChatEventDecoder();
      this.reading = { id: task.id, reader };
      let stopped = false;
      try {
        while (open(task) && !this.lifetime.signal.aborted) {
          const chunk = await reader.read();
          if (!open(task) || this.lifetime.signal.aborted || task.parentFinished) return;
          if (chunk.done) {
            if (stopped) return;
            break;
          }
          for (const frame of decoder.push(chunk.value)) {
            if (frame.event === 'message_stop') stopped = true;
            if (frame.event === 'content_block_delta') stopped = false;
            this.consume(task, frame.event, JSON.parse(frame.data));
          }
        }
      } catch (error) {
        if (!task.streamId || this.lifetime.signal.aborted || !(error instanceof TypeError)) throw error;
      } finally {
        this.reading = undefined;
        await reader.cancel().catch(() => {});
      }
    }
    if (!open(task) || this.lifetime.signal.aborted || task.parentFinished) return;
    if (!task.streamId) throw new Error('请求可能已提交，请在聊天中核对结果。');
    let cursor = 0,
      reset = true,
      retries = 0;
    while (open(task) && !this.lifetime.signal.aborted) {
      let active;
      try {
        active = await this.active(cursor);
        retries = 0;
      } catch (error) {
        if (!(error instanceof TypeError) || this.lifetime.signal.aborted || ++retries >= 5) throw error;
        await new Promise(resolve => setTimeout(resolve, 500 * 2 ** (retries - 1)));
        continue;
      }
      if (!active || active.stream_id !== task.streamId)
        throw new Error('原任务流已结束，请在聊天历史中核对结果。');
      if (!open(task) || this.lifetime.signal.aborted || task.parentFinished) return;
      if (reset) {
        task.result = '';
        task.activity = [];
        task.being = {};
        task.streamScene = active.scene_id ?? undefined;
        reset = false;
      }
      for (const item of active.events || []) {
        if (item.seq <= cursor) continue;
        if (item.seq !== cursor + 1) throw new Error('原任务流回放不完整，请在聊天中核对。');
        cursor = item.seq;
        this.consume(task, item.event, item.data);
      }
      if (active.finished) return;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
  private consume(task: Task, event: string, data: any) {
    if (!data || typeof data !== 'object') return;
    if (event === 'meta' && typeof data.stream_id === 'string') {
      if (task.streamId && task.streamId !== data.stream_id) throw new Error('原任务流已发生变化。');
      task.streamId = data.stream_id;
    }
    if ('scene_id' in data) task.streamScene = data.scene_id;
    if (event === 'meta') this.changed(task, 'task.running');
    if (task.streamScene !== undefined && task.streamScene !== task.scene.scene_id) return;
    if (event === 'content_block_delta' && typeof data.delta?.text === 'string') {
      task.result += data.delta.text;
      if (this.listening)
        this.options.emit({ type: 'task.output.delta', taskId: task.id, content: task.result });
    } else if (
      event === 'tool_use' ||
      event === 'tool_result' ||
      event === 'thinking' ||
      event === 'reasoning'
    ) {
      observeChildTool((task.being ??= {}), event, data);
      task.activity.push({
        type: event === 'tool_use' ? 'tool' : event === 'tool_result' ? 'tool-result' : 'think',
        name: data.name,
        text: typeof data.text === 'string' ? data.text.slice(0, 2000) : undefined,
        ts: Date.now(),
      });
      this.changed(task, 'task.progress');
    } else if (event === 'message_stop' && task.result) task.result += '\n\n';
    else if (event === 'error')
      throw new Error(typeof data.message === 'string' ? data.message : 'Being 任务执行失败。');
  }
}

async function confirmedStop(response: Response, streamId: string) {
  if (response.status === 204) return true;
  const receipt = await response.json().catch(() => null);
  return (
    response.ok &&
    response.status !== 202 &&
    receipt &&
    (receipt.ok === true || receipt.stopped === true) &&
    receipt.ok !== false &&
    receipt.stopped !== false &&
    (!receipt.stream_id || receipt.stream_id === streamId)
  );
}
