import { existsSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import type { ChatScene } from '../../shared/types';
import { pendingChildren } from '../../shared/subagent-receipts';
import type { NativeHistory } from './native-history';

type LegacyRow = Record<string, any>;

/** Read the previous Store.kv once. Its directory identifies the endpoint, but it contains no token. */
export function importLegacyVoiceState(
  history: NativeHistory,
  file: string,
  source: string,
  endpoint: string,
  tokenHash: string,
  notesScene: ChatScene,
  scenes: ChatScene[],
) {
  const marker = 'native-voice-import:' + source;
  if (history.get(marker) || !existsSync(file)) return;
  const db = new DatabaseSync(file, { readOnly: true });
  try {
    if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='kv'").get()) return;
    const rows = db.prepare("SELECT key,value FROM kv WHERE key IN ('notes','notes_v2','tasks')").all();
    const values = Object.fromEntries(rows.map((row) => [String(row.key), JSON.parse(String(row.value))]));
    history.transaction(() => {
      const prefix = 'native-voice-tasks:';
      const notes = values.notes_v2 ?? values.notes;
      if (notes && typeof notes === 'object') {
        const key = prefix + JSON.stringify([tokenHash + ':' + notesScene.scene_id, 'notes_v2']);
        if (!history.get(key)) history.set(key, notes);
      }
      const byScene = new Map<string, LegacyRow[]>();
      for (const task of Object.values<LegacyRow>(values.tasks ?? {})) {
        const scene = scenes.find((item) => item.scene_id === task?.scene?.scene_id);
        if (!scene || typeof task.id !== 'string' || typeof task.objective !== 'string') continue;
        const receipt = task.being;
        const correlated =
          receipt?.submitted === true &&
          receipt.endpoint === endpoint &&
          receipt.scene_id === scene.scene_id &&
          typeof receipt.stream_id === 'string' &&
          receipt.stream_id.length > 0;
        const row: LegacyRow = {
          id: task.id,
          turnId: task.turnId ?? task.id,
          scene,
          objective: task.objective,
          message: task.objective,
          status: task.status,
          createdAt: task.createdAt,
          startedAt: task.startedAt,
          completedAt: task.completedAt,
          result: typeof task.result === 'string' ? task.result : '',
          error: typeof task.error === 'string' ? task.error : undefined,
          activity: Array.isArray(task.activity) ? task.activity : [],
          notificationStatus: ['pending', 'delivered'].includes(task.notificationStatus)
            ? task.notificationStatus
            : undefined,
        };
        const schedule = task.schedule;
        if (
          task.status === 'scheduled' &&
          !receipt?.submitted &&
          !task.parts?.length &&
          Number.isFinite(schedule?.executeAt) &&
          ['reminder', 'task'].includes(schedule.type) &&
          ['once', 'daily', 'weekly', 'weekdays'].includes(schedule.recurrence ?? 'once')
        ) {
          row.schedule = {
            executeAt: schedule.executeAt,
            startAt: schedule.recurrenceStartAt ?? schedule.executeAt,
            recurrence: schedule.recurrence ?? 'once',
            timeZone: schedule.timeZone || Intl.DateTimeFormat().resolvedOptions().timeZone,
            type: schedule.type,
            seriesId: schedule.seriesId ?? task.seriesId ?? task.id,
          };
        } else if (!['completed', 'cancelled', 'failed'].includes(task.status)) {
          if (correlated && ['running', 'recovering', 'finalizing', 'delegated'].includes(task.status)) {
            row.submitted = true;
            row.streamId = receipt.stream_id;
            row.being = { subagents: receipt.subagents, subagentCalls: receipt.subagentCalls };
            row.status = task.status === 'delegated' && pendingChildren(row.being) ? 'delegated' : 'running';
            row.parentFinished = row.status === 'delegated';
          } else {
            row.status = 'failed';
            row.error = '旧任务缺少可安全恢复的回执，请在 Being 中核对。';
          }
        }
        const key = prefix + tokenHash + ':' + scene.scene_id;
        let tasks = byScene.get(key);
        if (!tasks) {
          tasks = history.get<LegacyRow[]>(key) ?? [];
          byScene.set(key, tasks);
        }
        if (!tasks.some((item) => item.id === row.id)) tasks.push(row);
      }
      for (const [key, tasks] of byScene) history.set(key, tasks);
      history.set(marker, true);
    });
  } finally {
    db.close();
  }
}
