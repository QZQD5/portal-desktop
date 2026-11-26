import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { ChatEventDecoder } from '../../shared/chat-sse';
import type { ChatScene } from '../../shared/types';
import type { Connection } from './connection';
import { NativeHistory, type ArchivedMessage } from './native-history';
import {
  callbackReply,
  cancelChildTasks,
  observeChildTool,
  pendingChildren,
  type ChildReceipt,
} from '../../shared/subagent-receipts';

type Data = Record<string, any>; // Being SSE payloads have event-specific fields; preserve unknown events on replay.
interface JournalEvent {
  seq: number;
  event: string;
  data: Data;
}
interface Journal extends ChildReceipt {
  stream_id: string;
  request_id: string;
  scene: ChatScene;
  scenes?: Record<string, ChatScene>;
  sequence: number;
  upstream_seq: number;
  upstream_id?: string;
  order: number;
  reply: number;
  text: string;
  finished: boolean;
  submitted: boolean;
  accepted?: boolean;
  stopped?: boolean;
  failed?: boolean;
  reply_scene?: ChatScene;
  stream_scene?: string;
  meta?: Data;
  last_persisted?: Data;
  callback?: boolean;
  discard_output?: boolean;
  opening?: string;
  callback_source_id?: string;
}
interface Stream {
  journal: Journal;
  events: JournalEvent[];
  abort: AbortController;
  listeners: Set<() => void>;
  reading: boolean;
}
interface ChatOptions {
  directory: string;
  binding(): { connection: Connection; scene: ChatScene } | null;
  scenes?(): ChatScene[];
  fetcher: typeof fetch;
  onReply?(sceneId: string, text: string, id: number): void;
  onSend?(sceneId: string): void;
  childStates?(sceneId: string): Record<string, string>;
  onChildResult?(
    sceneId: string,
    ids: string[],
    streamId: string,
    text: string,
    children: Record<string, string>,
  ): boolean;
}
interface Runtime {
  connection: Connection;
  history: NativeHistory;
  scene: ChatScene;
  current?: Stream;
  order: number;
  lifetime: AbortController;
  callbackPoll?: ReturnType<typeof setInterval>;
  polling: boolean;
  streams: Set<Stream>;
}
const error = (message: string, status: number) => Response.json({ error: message }, { status });
const openStream = (journal: Journal, events: JournalEvent[] = []): Stream => ({
  journal,
  events,
  abort: new AbortController(),
  listeners: new Set(),
  reading: false,
});

/** Being owns inference. This adapter journals SSE before delivery and never resubmits an uncertain POST. */
export class NativeChatGateway {
  private runtime?: Runtime;
  private initialPairing?: { endpoint: string; tokenHash: string; scene: ChatScene };
  constructor(private options: ChatOptions) {
    const saved = options.binding();
    if (saved)
      this.initialPairing = {
        endpoint: saved.connection.endpoint,
        tokenHash: createHash('sha256').update(saved.connection.token).digest('hex'),
        scene: saved.scene,
      };
  }

  private bound(): Runtime {
    const binding = this.options.binding();
    if (!binding) {
      this.dispose();
      throw new Error('请先连接 Being。');
    }
    const { connection, scene } = binding;
    if (
      this.runtime?.connection.endpoint === connection.endpoint &&
      this.runtime.connection.token === connection.token
    ) {
      this.runtime.scene = scene;
      return this.runtime;
    }
    this.dispose();
    const original = this.initialPairing;
    const history = new NativeHistory(
      this.options.directory,
      connection.endpoint,
      this.options.scenes?.() ?? [scene],
      original?.endpoint === connection.endpoint &&
        createHash('sha256').update(connection.token).digest('hex') === original.tokenHash
        ? original
        : undefined,
    );
    const previous = history.get<Journal>('chat');
    const runtime: Runtime = {
      connection,
      scene,
      history,
      lifetime: new AbortController(),
      order: previous?.order ?? 0,
      polling: false,
      streams: new Set(),
    };
    this.runtime = runtime;
    if (previous) {
      runtime.current = openStream(
        previous,
        history.db
          .prepare('SELECT body FROM chat_events ORDER BY seq')
          .all()
          .map((row) => JSON.parse(String(row.body))),
      );
      if (!previous.finished) this.read(runtime, runtime.current);
    }
    runtime.callbackPoll = setInterval(() => {
      void this.discover(runtime);
    }, 2000);
    runtime.callbackPoll.unref();
    return runtime;
  }

  ensure() {
    this.bound();
  }

  activeText(sceneId: string) {
    if (this.options.binding()?.scene.scene_id !== sceneId) return;
    const rt = this.bound(),
      j = rt.current?.journal;
    if (
      !j ||
      j.callback ||
      j.stopped ||
      (j.finished && !pendingChildren(j)) ||
      (j.reply_scene ?? j.scene).scene_id !== sceneId ||
      Object.keys(j.scenes ?? {}).some((id) => id !== sceneId)
    )
      return;
    return {
      stream_id: j.stream_id,
      scene_id: sceneId,
      status: j.finished ? 'awaiting_subagents' : 'running',
      order: rt.order,
    };
  }

  async stopText(sceneId: string, streamId: string, order?: number): Promise<Response> {
    const work = this.activeText(sceneId);
    if (!work || work.stream_id !== streamId || (order !== undefined && work.order !== order))
      return error('原文字任务已变化，请重新核对。', 409);
    return this.stop(this.runtime!, { stream_id: streamId });
  }

  recordVoice(identity: string, message: ArchivedMessage, endpoint: string) {
    if (this.options.binding()?.connection.endpoint === endpoint) this.bound().history.put(identity, message);
  }

  readVoiceTasks(endpoint: string, sceneId: string): unknown {
    if (this.options.binding()?.connection.endpoint !== endpoint) return;
    return this.bound().history.get('native-voice-tasks:' + sceneId);
  }

  saveVoiceTasks(endpoint: string, sceneId: string, tasks: unknown) {
    if (this.options.binding()?.connection.endpoint === endpoint)
      this.bound().history.set('native-voice-tasks:' + sceneId, tasks);
  }

  recentVoiceContext(endpoint: string, sceneId: string): { role: 'user' | 'assistant'; content: string }[] {
    if (this.options.binding()?.connection.endpoint !== endpoint) return [];
    const rows = this.bound()
      .history.db.prepare(
        `SELECT json_extract(body,'$.role') role, json_extract(body,'$.content') content
      FROM history WHERE scene=? AND json_extract(body,'$.role') IN ('user','assistant','being')
        AND length(json_extract(body,'$.content'))>0 AND coalesce(json_extract(body,'$.unconfirmed'),0)=0
        AND coalesce(json_extract(body,'$.voicePending'),0)=0 AND coalesce(json_extract(body,'$.from'),'')<>'partial'
      ORDER BY seq DESC LIMIT 16`,
      )
      .all(sceneId);
    return rows
      .reverse()
      .map((row) => ({ role: row.role === 'user' ? 'user' : 'assistant', content: String(row.content) }));
  }

  localHistory(path: string, init: RequestInit) {
    const endpoint = new Headers(init.headers).get('X-Portal-Being-Endpoint');
    if (endpoint && this.options.binding()?.connection.endpoint !== endpoint)
      return Promise.resolve(error('Being 连接已切换。', 409));
    const rt = this.bound();
    return rt.history.handle(new URL(path, 'beings://chat'), { ...init, signal: rt.lifetime.signal });
  }

  private request(rt: Runtime, route: string, init: RequestInit = {}) {
    const url = new URL(rt.connection.endpoint + route);
    url.searchParams.set('token', rt.connection.token);
    return this.options.fetcher(url.href, {
      ...init,
      redirect: 'error',
      credentials: 'omit',
      cache: 'no-store',
      signal: init.signal ? AbortSignal.any([rt.lifetime.signal, init.signal]) : rt.lifetime.signal,
    });
  }

  async fetchChat(input: RequestInfo | URL, init: RequestInit = {}): Promise<Response> {
    const rt = this.bound();
    const url = new URL(input instanceof Request ? input.url : input.toString());
    if (!url.href.startsWith(rt.connection.endpoint + '/')) return error('Being 连接已切换。', 409);
    const route = url.pathname.slice(new URL(rt.connection.endpoint).pathname.length);
    if (route === '/api/history') return rt.history.handle(url, init);
    if (route === '/api/stream/active') {
      const stream = rt.current;
      if (!stream) return new Response(null, { status: 204 });
      const j = stream.journal;
      return Response.json({
        ...j.meta,
        stream_id: j.stream_id,
        ...j.scene,
        finished: j.finished,
        next_seq: j.sequence + 1,
        ...(j.stopped && { stopped: true }),
        ...(j.discard_output && { discarded: true }),
        ...(pendingChildren(j) && { awaiting_subagents: true }),
        events: stream.events.filter(
          (e) =>
            e.seq > Number(url.searchParams.get('after') ?? 0) &&
            (!j.discard_output || e.event === 'stream_discarded' || e.event === 'stream_stopped'),
        ),
      });
    }
    if (route !== '/api/chat/stream' && route !== '/api/stop') return error('Unknown route', 404);
    const body = await new Response(init.body).json().catch(() => null);
    if (rt !== this.runtime) return error('Being 连接已切换。', 409);
    if (route === '/api/stop') return this.stop(rt, body);
    if (
      !body ||
      typeof body.message !== 'string' ||
      typeof body.scene_id !== 'string' ||
      !body.scene_id ||
      body.scene_id.length > 128 ||
      !body.scene_meta ||
      typeof body.scene_meta !== 'object'
    )
      return error('Invalid chat message or scene', 400);
    return this.submit(rt, body);
  }

  private save(rt: Runtime, stream: Stream, item?: JournalEvent) {
    const j = stream.journal;
    if (stream === rt.current) {
      rt.history.set('chat', j);
      if (item) {
        const db = rt.history.db;
        db.prepare('INSERT OR REPLACE INTO chat_events VALUES (?,?)').run(item.seq, JSON.stringify(item));
        if (item.seq > 10000) db.prepare('DELETE FROM chat_events WHERE seq<=?').run(item.seq - 10000);
      }
    }
    if (j.callback) rt.history.set('callback:' + j.upstream_id, j.finished ? { finished: true } : j);
  }

  private activate(rt: Runtime, stream: Stream) {
    rt.current = stream;
    rt.history.db.exec('DELETE FROM chat_events');
    this.save(rt, stream);
  }

  private emit(rt: Runtime, stream: Stream, event: string, data: Data) {
    const j = stream.journal;
    const item = {
      seq: ++j.sequence,
      event,
      data: { ...data, stream_id: j.stream_id, ...(j.reply_scene ?? j.scene) },
    };
    stream.events.push(item);
    if (stream.events.length > 10000) stream.events.shift();
    this.save(rt, stream, item);
    for (const wake of stream.listeners) wake();
  }

  private archive(rt: Runtime, stream: Stream, partial: boolean, from = 'final', beingSession?: string) {
    const j = stream.journal,
      scene = j.reply_scene ?? j.scene;
    if (!j.text || j.discard_output) return;
    const id = rt.history.put(`text:${j.stream_id}:${j.reply}`, {
      role: 'assistant',
      content: j.text,
      source: j.callback ? 'portal-callback' : 'text-being',
      from: partial ? 'partial' : from,
      scene_id: scene.scene_id || '',
      scene_label: scene.scene_meta?.scene_label || '',
      ...(beingSession && { beingSessionId: beingSession }),
    });
    return { kind: 'self', seq: id, stream_id: j.stream_id, from: partial ? 'partial' : from };
  }

  private consume(rt: Runtime, stream: Stream, event: string, raw: Data, seq?: number) {
    const j = stream.journal,
      data = { ...raw };
    const persisted = data.persisted;
    delete data.persisted;
    const counted = event !== 'meta' || data.continuation === true || !!persisted;
    j.upstream_seq = seq ?? j.upstream_seq + Number(counted);
    if (event === 'meta' && data.stream_id) {
      if (j.upstream_id && data.stream_id !== j.upstream_id) throw new Error('Stream identity changed');
      j.upstream_id = data.stream_id;
    }
    if ('scene_id' in data) j.stream_scene = data.scene_id;
    const sceneId = j.stream_scene ?? j.scene.scene_id;
    const scene = sceneId === j.scene.scene_id ? j.scene : j.scenes?.[sceneId];
    if (scene) observeChildTool(j, event, data);
    if (!scene || j.discard_output) {
      this.save(rt, stream);
      return;
    }
    if (scene.scene_id !== (j.reply_scene ?? j.scene).scene_id && j.text) {
      this.emit(rt, stream, 'message_stop', { persisted: this.archive(rt, stream, true) });
      j.reply++;
      j.text = '';
    }
    j.reply_scene = scene;
    if (event === 'error') j.failed = true;
    if (event === 'content_block_delta' && typeof data.delta?.text === 'string') j.text += data.delta.text;
    if (event === 'message_stop' || persisted?.kind === 'self') {
      const saved = this.archive(rt, stream, !!j.failed, persisted?.from ?? 'final', data.session_id);
      if (saved) {
        j.last_persisted = saved;
        data.persisted = saved;
        if (!j.failed) this.options.onReply?.(scene.scene_id, j.text, saved.seq);
        j.reply++;
        j.text = '';
      } else if (event === 'message_stop' && j.last_persisted) data.persisted = j.last_persisted;
    }
    if (counted) this.emit(rt, stream, event, data);
    else {
      j.meta = { ...data, ...(j.callback && { origin: 'callback' }) };
      this.save(rt, stream);
      for (const wake of stream.listeners) wake();
    }
  }

  private async submit(rt: Runtime, body: Data): Promise<Response> {
    const { client_request_id, ...upstream } = body;
    const id: string = client_request_id ?? randomUUID();
    if (typeof id !== 'string' || !id || id.length > 128) return error('Invalid request ID', 400);
    const key = 'request:' + id,
      receipt = rt.history.get<Data>(key);
    if (receipt?.uncertain || receipt?.status === 'sending')
      return error('原请求结果尚未确认，不会自动重复提交。', 502);
    if (receipt && !(typeof receipt.status === 'number' && receipt.status >= 400))
      return new Response(null, { status: 202 });
    const scene = { scene_id: body.scene_id, scene_meta: body.scene_meta };
    const order = ++rt.order;
    const create = () =>
      openStream({
        stream_id: 'local-' + randomUUID().replaceAll('-', ''),
        request_id: id,
        scene,
        sequence: 0,
        upstream_seq: 0,
        reply: 0,
        text: '',
        order,
        finished: false,
        submitted: true,
      });
    let stream = rt.current;
    if (stream?.journal.discard_output && !stream.journal.finished)
      return error('上一条文字请求仍在结束，请稍后发送。', 409);
    this.options.onSend?.(scene.scene_id);
    rt.history.transaction(() => {
      rt.history.put(key, {
        role: 'user',
        content: body.message,
        source: 'text-user',
        scene_id: scene.scene_id,
        scene_label: scene.scene_meta.scene_label ?? '',
        ...(body.session_id && { beingSessionId: body.session_id }),
      });
      if (stream && !stream.journal.finished) {
        const j = stream.journal;
        (j.scenes ??= { [j.scene.scene_id]: j.scene })[scene.scene_id] = scene;
        this.save(rt, stream);
      } else {
        stream = create();
        stream.journal.opening = id;
        this.activate(rt, stream);
      }
      rt.history.set(key, { status: 'sending' });
    });
    let response: Response;
    try {
      response = await this.request(rt, '/api/chat/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(upstream),
      });
    } catch {
      if (rt === this.runtime) {
        rt.history.set(key, { uncertain: true });
        if (stream!.journal.opening === id) this.endOpening(rt, stream!);
      }
      return error('发送结果无法确认，请核对记录；不会自动重复提交。', 502);
    }
    if (rt !== this.runtime) {
      await response.body?.cancel();
      return error('Being 连接已切换。', 409);
    }
    stream = stream!;
    const j = stream.journal;
    if (response.status === 202) {
      stream = rt.current!;
      const active = stream.journal;
      (active.scenes ??= { [active.scene.scene_id]: active.scene })[scene.scene_id] = scene;
      active.accepted = true;
      active.finished = false;
      if (active.opening === id) delete active.opening;
      rt.history.transaction(() => {
        rt.history.set(key, { stream_id: active.stream_id });
        this.save(rt, stream!);
      });
      if (!active.opening && !stream.reading) this.read(rt, stream);
      return response;
    }
    if (
      response.status !== 200 ||
      !response.headers.get('content-type')?.includes('text/event-stream') ||
      !response.body
    ) {
      rt.history.set(key, { status: response.status });
      if (j.opening === id) this.endOpening(rt, stream);
      return response;
    }
    if (j.opening === id) delete j.opening;
    else {
      stream = create();
      if (order >= (rt.current?.journal.order ?? 0)) rt.history.transaction(() => this.activate(rt, stream!));
    }
    rt.history.transaction(() => {
      rt.history.set(key, { stream_id: stream!.journal.stream_id });
      this.save(rt, stream!);
    });
    const local = this.response(stream, response.headers);
    this.read(rt, stream, response);
    return local;
  }

  private endOpening(rt: Runtime, stream: Stream) {
    const j = stream.journal;
    delete j.opening;
    j.finished = !j.accepted;
    j.failed = j.finished;
    this.save(rt, stream);
    if (j.accepted) this.read(rt, stream);
  }

  private response(stream: Stream, headers: Headers) {
    const encoder = new TextEncoder();
    let after = 0,
      lastMeta: Data | undefined,
      wake: (() => void) | undefined,
      cancelled = false;
    return new Response(
      new ReadableStream<Uint8Array>({
        async pull(controller) {
          while (!cancelled) {
            const j = stream.journal;
            if (j.meta && j.meta !== lastMeta) {
              lastMeta = j.meta;
              controller.enqueue(
                encoder.encode(
                  'event: meta\ndata: ' + JSON.stringify({ ...j.meta, stream_id: j.stream_id }) + '\n\n',
                ),
              );
              return;
            }
            const messages = stream.events.filter((item) => item.seq > after);
            if (messages.length) {
              after = messages[messages.length - 1].seq;
              controller.enqueue(
                encoder.encode(
                  messages
                    .map((item) => 'event: ' + item.event + '\ndata: ' + JSON.stringify(item.data) + '\n\n')
                    .join(''),
                ),
              );
              return;
            }
            if (j.finished || stream.abort.signal.aborted) {
              controller.close();
              return;
            }
            await new Promise<void>((resolve) => {
              wake = resolve;
              stream.listeners.add(resolve);
            });
            stream.listeners.delete(wake!);
            wake = undefined;
          }
        },
        cancel() {
          cancelled = true;
          wake?.();
        },
      }),
      { headers: { 'content-type': headers.get('content-type')!, 'cache-control': 'no-store' } },
    );
  }

  private read(rt: Runtime, stream: Stream, response?: Response, snapshot?: Data) {
    if (stream.reading) return;
    stream.reading = true;
    rt.streams.add(stream);
    void (async () => {
      const j = stream.journal;
      try {
        if (response?.body) {
          const reader = response.body.getReader(),
            decoder = new ChatEventDecoder();
          const cancel = () => {
            void reader.cancel().catch(() => {});
          };
          stream.abort.signal.addEventListener('abort', cancel, { once: true });
          try {
            for (;;) {
              const chunk = await reader.read();
              if (stream.abort.signal.aborted || rt.lifetime.signal.aborted) return;
              if (chunk.done) break;
              for (const entry of decoder.push(chunk.value)) {
                const data = JSON.parse(entry.data);
                if (data && typeof data === 'object')
                  rt.history.transaction(() => this.consume(rt, stream, entry.event, data));
              }
            }
          } catch (cause) {
            if (!j.upstream_id || stream.abort.signal.aborted) throw cause;
            await this.recover(rt, stream);
          } finally {
            stream.abort.signal.removeEventListener('abort', cancel);
            reader.releaseLock();
          }
        } else await this.recover(rt, stream, snapshot);
      } catch {
        if (stream.abort.signal.aborted || rt.lifetime.signal.aborted) return;
        j.failed = true;
        this.emit(rt, stream, 'error', {
          error: '连接中断，无法确认原请求结果；本地记录已保留，不会自动重发。',
        });
      } finally {
        if (!stream.abort.signal.aborted && !rt.lifetime.signal.aborted) {
          rt.history.transaction(() => {
            const saved = this.archive(rt, stream, !!j.failed);
            if (saved) {
              this.emit(rt, stream, 'message_stop', { persisted: saved });
              if (!j.failed) this.options.onReply?.((j.reply_scene ?? j.scene).scene_id, j.text, saved.seq);
            }
            j.finished = true;
            this.save(rt, stream);
          });
        }
        stream.reading = false;
        rt.streams.delete(stream);
        for (const wake of stream.listeners) wake();
      }
    })();
  }

  private async recover(rt: Runtime, stream: Stream, snapshot?: Data) {
    const j = stream.journal;
    if (!j.upstream_id && !j.accepted) throw new Error('No correlated stream');
    while (!stream.abort.signal.aborted) {
      const cursor = j.upstream_seq;
      let active = snapshot;
      snapshot = undefined;
      if (!active) {
        const response = await this.request(rt, '/api/stream/active?after=' + cursor, {
          signal: stream.abort.signal,
        });
        if (!response.ok) throw new Error('Recovery failed');
        if (response.status !== 204) active = await response.json();
      }
      stream.abort.signal.throwIfAborted();
      if (active && j.accepted && j.upstream_id !== active.stream_id) {
        j.upstream_id = active.stream_id;
        j.stream_scene = active.scene_id;
        j.upstream_seq = 0;
        this.save(rt, stream);
        if (cursor) continue;
      }
      if (active) {
        if (active.stream_id !== j.upstream_id) throw new Error('Original stream unavailable');
        const items: JournalEvent[] = (active.events ?? []).filter(
          (item: JournalEvent) => item.seq > j.upstream_seq,
        );
        if (items.some((item, index) => item.seq !== j.upstream_seq + index + 1))
          throw new Error('Replay gap');
        if (j.callback) {
          let start = 0;
          for (let i = 0; i < items.length; i++) {
            if (items[i].event !== 'message_stop' && !(active.finished && i === items.length - 1)) continue;
            const batch = items.slice(start, i + 1);
            const meta = batch.filter((item) => item.event === 'meta' && 'scene_id' in item.data).at(-1);
            const scene = meta ? meta.data.scene_id : (j.stream_scene ?? active.scene_id ?? '');
            const reply = callbackReply(
              batch,
              j.callback_source_id ?? active.task_id,
              this.options.childStates?.(scene || ''),
            );
            const text = reply.items
              .filter((item) => item.event === 'content_block_delta')
              .map((item) => item.data.delta?.text ?? '')
              .join('');
            rt.history.transaction(() => {
              j.callback_source_id = reply.sourceId;
              if (
                reply.ids.length &&
                this.options.onChildResult?.(scene || '', reply.ids, j.upstream_id!, text, reply.children)
              ) {
                j.upstream_seq = batch[batch.length - 1].seq;
                j.reply++;
                this.save(rt, stream);
              } else
                for (const item of reply.items) this.consume(rt, stream, item.event, item.data, item.seq);
            });
            start = i + 1;
          }
        } else
          for (const item of items)
            rt.history.transaction(() => this.consume(rt, stream, item.event, item.data, item.seq));
        if (active.finished && (!j.accepted || j.reply > 0)) return;
      } else if (!j.accepted) throw new Error('Original stream unavailable');
      await delay(500, undefined, { signal: stream.abort.signal });
    }
  }

  private async discover(rt: Runtime) {
    if (rt.polling) return;
    rt.polling = true;
    const previous = rt.current,
      order = rt.order;
    try {
      const response = await this.request(rt, '/api/stream/active', { signal: AbortSignal.timeout(15000) });
      if (response.status !== 200) return;
      const active = await response.json();
      if (
        rt !== this.runtime ||
        active?.origin !== 'callback' ||
        typeof active.stream_id !== 'string' ||
        !Array.isArray(active.events)
      )
        return;
      const checkpoint = rt.history.get<Journal>('callback:' + active.stream_id);
      if (
        checkpoint?.finished ||
        Array.from(rt.streams).some((stream) => stream.journal.upstream_id === active.stream_id)
      )
        return;
      const shared = { scene_id: '', scene_meta: { client: 'desktop', scene_label: '' } };
      const scenes = Object.fromEntries(
        [shared, ...(this.options.scenes?.() ?? [rt.scene])].map((row) => [row.scene_id, row]),
      );
      const stream = openStream(
        checkpoint ?? {
          stream_id: 'local-' + randomUUID().replaceAll('-', ''),
          request_id: 'callback:' + active.stream_id,
          scene: scenes[active.scene_id] ?? shared,
          scenes,
          stream_scene: active.scene_id,
          sequence: 0,
          upstream_seq: 0,
          upstream_id: active.stream_id,
          order: rt.order,
          reply: 0,
          text: '',
          finished: false,
          submitted: true,
          callback: true,
          meta: { origin: 'callback' },
        },
      );
      rt.history.transaction(() => {
        if (rt.current === previous && rt.order === order && (!previous || previous.journal.finished))
          this.activate(rt, stream);
        else this.save(rt, stream);
      });
      this.read(rt, stream, undefined, active);
    } catch {
      /* Poll again on the next tick. Never expose credential-bearing fetch errors. */
    } finally {
      rt.polling = false;
    }
  }

  private async stop(rt: Runtime, body: Data | null) {
    const stream = rt.current;
    if (!stream || !body || body.stream_id !== stream.journal.stream_id)
      return error('Unknown local stream', 409);
    const j = stream.journal;
    let result = Response.json({ ok: true, stream_id: j.stream_id });
    if (j.finished && (j.callback || !pendingChildren(j))) return result;
    if (!j.finished) {
      if (!j.upstream_id) return error('Being 尚未返回流标识，请稍后停止。', 409);
      const response = await this.request(rt, '/api/stop', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ ...body, stream_id: j.upstream_id }),
        signal: AbortSignal.timeout(15000),
      });
      if (!response.ok || response.status === 202) return response;
      const receipt =
        response.status === 204
          ? null
          : await response
              .clone()
              .json()
              .catch(() => null);
      if (
        response.status !== 204 &&
        (!receipt ||
          (receipt.ok !== true && receipt.stopped !== true) ||
          receipt.ok === false ||
          receipt.stopped === false ||
          (receipt.stream_id && receipt.stream_id !== j.upstream_id))
      )
        return response;
      if (rt !== this.runtime) return error('Being 连接已切换。', 409);
      stream.abort.abort();
      rt.history.transaction(() => {
        this.archive(rt, stream, true);
        j.finished = true;
        this.save(rt, stream);
      });
      result = receipt?.stream_id
        ? Response.json({ ...receipt, stream_id: j.stream_id }, { status: response.status })
        : response;
    }
    if (!j.callback && pendingChildren(j)) {
      let unconfirmed: Response | undefined;
      try {
        unconfirmed = await cancelChildTasks(j, j.scene, (route, value) =>
          this.request(rt, route, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(value),
            signal: AbortSignal.timeout(30000),
          }),
        );
      } catch {
        unconfirmed = error('子代理取消结果尚未确认，请在 Being 聊天中核对。', 409);
      }
      if (rt !== this.runtime) return error('Being 连接已切换。', 409);
      this.save(rt, stream);
      if (unconfirmed) return unconfirmed;
    }
    if (!j.stopped) {
      j.stopped = true;
      this.emit(rt, stream, 'stream_stopped', {});
    }
    return result;
  }

  dispose() {
    const rt = this.runtime;
    if (!rt) return;
    this.runtime = undefined;
    clearInterval(rt.callbackPoll);
    rt.lifetime.abort();
    for (const stream of rt.streams) {
      stream.abort.abort();
      for (const wake of stream.listeners) wake();
    }
    rt.history.db.close();
  }
}
