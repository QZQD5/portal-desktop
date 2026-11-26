import { HistoryCache, historyCacheDatabaseName, type HistoryMessage } from './history-cache';
import type { SceneSummary } from './client-commands';

const migrations = new Map<string, Promise<void>>();

/** Desktop's history is owned by Gateway; IndexedDB is read only for legacy import. */
export class GatewayHistory {
  constructor(private endpoint: string, private request: typeof fetch = globalThis.fetch.bind(globalThis)) {}

  private async query(path: string, init: RequestInit = {}) {
    const response = await this.request('beings://chat/api/local-history' + path, { ...init,
      headers: { 'Content-Type': 'application/json', 'X-Portal-Being-Endpoint': this.endpoint },
      signal: init.signal || AbortSignal.timeout(60000), cache: 'no-store' });
    if (!response.ok) throw new Error('本地历史读取或迁移失败，请重试。');
    return response.json();
  }

  private migrate() {
    if (!migrations.has(this.endpoint)) {
      const migration = (async () => {
        if (typeof indexedDB === 'undefined') return;
        if (typeof indexedDB.databases === 'function') {
          const name = historyCacheDatabaseName(this.endpoint, true);
          if (!(await indexedDB.databases()).some(database => database.name === name)) return;
        }
        const upload = (messages: HistoryMessage[], complete = false) => this.query('/import', {
          method: 'POST', body: JSON.stringify({ source:'heart-indexeddb', messages, complete }),
        });
        const status = await upload([]);
        if (status.complete === true) return;
        const legacy = new HistoryCache(this.endpoint, true);
        try {
          const saved = await legacy.read();
          if (!saved) throw new Error('旧本地历史读取失败，原数据已保留。');
          // Keep original IndexedDB intact; server import is idempotent by upstream seq.
          let batch: HistoryMessage[] = [], size = 0;
          for (const row of saved.messages) {
            const bytes = new TextEncoder().encode(JSON.stringify(row)).length;
            if (batch.length && (batch.length >= 200 || size + bytes > 700_000)) {
              await upload(batch);
              batch = []; size = 0;
            }
            batch.push(row); size += bytes;
          }
          await upload(batch, true);
        } finally { legacy.close(); }
      })().catch(error => { migrations.delete(this.endpoint); throw error; });
      migrations.set(this.endpoint, migration);
    }
    return migrations.get(this.endpoint)!;
  }

  private async page(params: Record<string, string | number>) {
    await this.migrate();
    const value = await this.query('?' + new URLSearchParams(Object.entries(params).map(([key, value]) => [key, String(value)])));
    if (!value || !Array.isArray(value.messages) || !Number.isSafeInteger(value.lastSeq) ||
        value.messages.some((row: HistoryMessage) => !Number.isSafeInteger(row.seq) || row.seq <= 0 || typeof row.content !== 'string'))
      throw new Error('本地历史响应格式无效。');
    return value;
  }

  async read(limit = Infinity): Promise<{ messages: HistoryMessage[]; lastSeq: number }> {
    if (Number.isFinite(limit)) return this.page({ limit: Math.min(500, limit) });
    let after = 0, lastSeq = 0;
    const messages: HistoryMessage[] = [];
    while (true) {
      const page = await this.page({ after, limit: 500 });
      lastSeq = page.lastSeq;
      messages.push(...page.messages);
      if (page.messages.length < 500) break;
      const next = page.messages[page.messages.length - 1].seq;
      if (next <= after) throw new Error('本地历史游标未前进。');
      after = next;
    }
    return { messages, lastSeq };
  }

  async readBefore(before: number, limit: number, include: (message: HistoryMessage) => boolean = () => true) {
    const messages: HistoryMessage[] = [];
    while (messages.length < limit) {
      const page = await this.page({ ...(Number.isFinite(before) ? { before } : {}), limit: 500 });
      if (!page.messages.length) break;
      messages.unshift(...(page.messages as HistoryMessage[]).filter(include).slice(-(limit - messages.length)));
      const next = page.messages[0].seq;
      if (next >= before) throw new Error('本地历史游标未前进。');
      before = next;
      if (page.messages.length < 500) break;
    }
    return messages;
  }

  async readAround(seq: number, sceneId: string, radius = 10): Promise<HistoryMessage[]> {
    const [before, after] = await Promise.all([
      this.page({ before: seq, scene: sceneId, limit: radius }),
      this.page({ after: seq - 1, scene: sceneId, limit: radius + 1 }),
    ]);
    return [...before.messages, ...after.messages];
  }

  async readScene(sceneId: string, limit = Infinity): Promise<HistoryMessage[]> {
    if (Number.isFinite(limit)) return (await this.page({ scene: sceneId, limit: Math.min(500, limit) })).messages;
    const messages: HistoryMessage[] = [];
    let after = 0;
    while (true) {
      const page = await this.page({ scene: sceneId, after, limit: 500 });
      messages.push(...page.messages);
      if (page.messages.length < 500) return messages;
      const next = page.messages[page.messages.length - 1].seq;
      if (next <= after) throw new Error('本地历史游标未前进。');
      after = next;
    }
  }

  context(sceneId: string, limit = 50) { return this.readScene(sceneId, Math.min(50, limit)); }
  async scenes(): Promise<SceneSummary[]> { await this.migrate(); return (await this.query('/scenes')).scenes; }
  async merge(messages: HistoryMessage[]) { return messages; }
  async write(_messages: HistoryMessage[], _cursor: number) { /* Gateway archives before emitting events. */ }
  close() { /* No renderer-owned database or connection. */ }
}

export function createHistory(endpoint: string) {
  return typeof location !== 'undefined' && location.protocol === 'beings:'
    ? new GatewayHistory(endpoint) : new HistoryCache(endpoint);
}
