import { createHash } from 'node:crypto';
import { existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import type { ChatScene } from '../../shared/types';
import { importLegacyVoiceState } from './legacy-voice-state';

export interface ArchivedMessage {
  role: string;
  content: string;
  scene_id: string;
  scene_label?: string;
  [key: string]: unknown;
}

/** Retains the Python Gateway's on-disk format, sequence space and identities. */
export class NativeHistory {
  readonly db: DatabaseSync;

  constructor(
    directory: string,
    endpoint: string,
    scenes: ChatScene[],
    original?: { tokenHash: string; scene: ChatScene },
  ) {
    const scope = createHash('sha256').update(endpoint).digest('hex');
    const parent = path.join(directory, 'gateway', scope);
    mkdirSync(parent, { recursive: true });
    this.db = new DatabaseSync(path.join(parent, 'history.sqlite3'));
    try {
      this.db.exec(`PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS history (seq INTEGER PRIMARY KEY AUTOINCREMENT,
          identity TEXT UNIQUE NOT NULL, scene TEXT NOT NULL, body TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS history_scene ON history(scene, seq);
        CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS chat_events (seq INTEGER PRIMARY KEY, body TEXT NOT NULL);`);
      if (original)
        importLegacyVoiceState(
          this,
          path.join(parent, 'gateway.sqlite3'),
          'endpoint',
          endpoint,
          original.tokenHash,
          original.scene,
          scenes,
        );
      for (const scene of scenes) {
        const id = createHash('sha256')
          .update(endpoint + '\n' + scene.scene_id)
          .digest('hex');
        const legacy = path.join(directory, 'voice', id, 'gateway.sqlite3');
        if (original) importLegacyVoiceState(this, legacy, id, endpoint, original.tokenHash, scene, [scene]);
        if (!existsSync(legacy) || this.get('native-legacy-import:' + id)) continue;
        const source = new DatabaseSync(legacy, { readOnly: true });
        try {
          this.transaction(() => {
            for (const row of source.prepare('SELECT body FROM messages ORDER BY id').iterate()) {
              const m = JSON.parse(String(row.body));
              if (
                this.get('deleted-session:' + m.sessionId) ||
                m.unconfirmed ||
                m.source === 'agent-presentation' ||
                (m.partialTranscript && m.playbackStatus === 'started')
              )
                continue;
              let identity = 'voice:' + m.sessionId + ':' + m.id;
              if (m.source === 'voice-assistant') {
                m.voiceKey = m.id;
                m.voicePending = ['not_started', 'started'].includes(m.playbackStatus);
              }
              if (m.source === 'agent-result' && m.taskId) {
                identity = 'task-result:' + m.taskId;
                m.voiceKey = 'agent:' + m.taskId;
                m.sourceSessionId ??= m.sessionId;
              }
              // The live archive can contain a newer playback receipt than the old database.
              if (this.db.prepare('SELECT 1 FROM history WHERE identity=?').get(identity)) continue;
              m.scene_id ??= scene.scene_id;
              m.scene_label = m.scene_meta?.scene_label ?? scene.scene_meta.scene_label;
              if (typeof m.createdAt === 'number') m.at = new Date(m.createdAt).toISOString();
              this.put(identity, m);
            }
            this.set('native-legacy-import:' + id, true);
          });
        } finally {
          source.close();
        }
      }
    } catch (error) {
      this.db.close();
      throw error;
    }
  }

  transaction<T>(action: () => T): T {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = action();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  get<T>(key: string): T | undefined {
    const row = this.db.prepare('SELECT body FROM state WHERE key=?').get(key);
    return row && JSON.parse(String(row.body));
  }

  set(key: string, value: unknown) {
    this.db.prepare('INSERT OR REPLACE INTO state (key,body) VALUES (?,?)').run(key, JSON.stringify(value));
  }

  put(identity: string, message: ArchivedMessage) {
    const { seq: _seq, ...body } = message;
    body.id ??= identity;
    body.at ??= new Date().toISOString();
    const row = this.db
      .prepare(
        `INSERT INTO history(identity,scene,body) VALUES(?,?,?)
      ON CONFLICT(identity) DO UPDATE SET scene=excluded.scene,body=excluded.body RETURNING seq`,
      )
      .get(identity, body.scene_id, JSON.stringify(body))!;
    return Number(row.seq);
  }

  async handle(url: URL, init: RequestInit): Promise<Response> {
    if (url.pathname.endsWith('/import')) {
      if (init.method !== 'POST') return new Response(null, { status: 405 });
      const body = await new Response(init.body).json().catch(() => null);
      if (init.signal?.aborted) return Response.json({ error: 'Being 连接已切换。' }, { status: 409 });
      const rows = body?.messages;
      const completed =
        body?.source === 'heart-indexeddb'
          ? 'heart-indexeddb-import-complete'
          : 'legacy-desktop-import-complete';
      if (
        !Array.isArray(rows) ||
        rows.length > 500 ||
        !rows.every(
          (row) =>
            row &&
            Number.isSafeInteger(row.seq) &&
            row.seq > 0 &&
            typeof row.content === 'string' &&
            row.content.length <= 1_000_000 &&
            ['user', 'assistant', 'being', 'system'].includes(row.role) &&
            typeof (row.scene_id ?? '') === 'string',
        )
      )
        return Response.json({ error: 'Invalid legacy history record' }, { status: 400 });
      this.transaction(() => {
        for (const row of rows) {
          const id = 'heart:' + row.seq;
          if (!this.db.prepare('SELECT 1 FROM history WHERE identity=?').get(id)) {
            this.put(id, {
              ...row,
              scene_id: row.scene_id ?? '',
              source: 'legacy-desktop',
              upstream_seq: row.seq,
            });
          }
        }
        if (body.complete === true) this.set(completed, true);
      });
      return Response.json({ ok: true, complete: this.get(completed) === true });
    }
    if (url.pathname.endsWith('/scenes')) {
      const scenes = this.db
        .prepare(
          `SELECT scene AS sceneId, count(*) AS messageCount, max(json_extract(body,'$.at')) AS lastActive,
        (SELECT coalesce(json_extract(recent.body, '$.scene_label'), '') FROM history recent
          WHERE recent.scene=h.scene ORDER BY recent.seq DESC LIMIT 1) AS label FROM history h WHERE scene<>'' GROUP BY scene`,
        )
        .all();
      return Response.json({ scenes });
    }
    const query = url.searchParams;
    const clauses: string[] = [],
      args: (number | string)[] = [];
    for (const [name, condition] of [
      ['after', 'seq>?'],
      ['before', 'seq<?'],
    ] as const) {
      if (!query.has(name)) continue;
      const seq = Number(query.get(name));
      if (!Number.isSafeInteger(seq))
        return Response.json({ error: 'Invalid history cursor' }, { status: 400 });
      clauses.push(condition);
      args.push(seq);
    }
    if (query.has('scene')) {
      clauses.push('scene=?');
      args.push(query.get('scene')!);
    }
    const ascending = query.has('after');
    const limit = Number(query.get('limit') ?? 100);
    if (!Number.isSafeInteger(limit))
      return Response.json({ error: 'Invalid history limit' }, { status: 400 });
    const rows = this.db
      .prepare(
        'SELECT seq,body FROM history' +
          (clauses.length ? ' WHERE ' + clauses.join(' AND ') : '') +
          ' ORDER BY seq ' +
          (ascending ? 'ASC' : 'DESC') +
          ' LIMIT ?',
      )
      .all(...args, Math.max(1, Math.min(limit, 500)));
    if (!ascending) rows.reverse();
    const messages = rows.map((row) => ({ ...JSON.parse(String(row.body)), seq: row.seq }));
    const lastSeq = this.db.prepare('SELECT coalesce(max(seq),0) AS seq FROM history').get()!.seq;
    return Response.json({ messages, lastSeq });
  }
}
