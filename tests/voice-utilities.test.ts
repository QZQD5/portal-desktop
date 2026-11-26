import { afterEach, expect, it, vi } from 'vitest';
import { VoiceTasks } from '../desktop/shared/voice-tasks';
import { nextOccurrence, voiceNotes } from '../desktop/shared/voice-utilities';
import type { VoiceToolCall } from '../desktop/shared/voice-realtime';
import type { ChatScene } from '../desktop/shared/types';

afterEach(() => vi.useRealTimers());

it('reads legacy lists and keeps ambiguity, duplicates, and sensitive items out of writes', async () => {
  const values = new Map<string, unknown>([['notes', { 购物: ['牛奶', '低脂牛奶'] }]]);
  const store = {
    read: (key: string) => values.get(key),
    write: (key: string, value: unknown) => values.set(key, value),
  };
  expect(await voiceNotes(store, { action: 'remove', list: '购物', items: ['奶'] })).toMatchObject({
    status: 'ambiguous',
  });
  expect(values.has('notes_v2')).toBe(false);
  expect(await voiceNotes(store, { action: 'add', list: '购', items: ['牛奶', '面包'] })).toMatchObject({
    list: '购物',
    added: ['面包'],
    duplicates: ['牛奶'],
  });
  const result = await voiceNotes(store, { action: 'show', list: '购' });
  expect(result.items).toContainEqual({ id: expect.stringMatching(/^item_[0-9a-f]{12}$/), text: '面包' });
  await expect(voiceNotes(store, { action: 'add', list: '购物', items: ['密码 123'] })).rejects.toThrow(
    '不能保存',
  );
  expect(await voiceNotes(store, { action: 'clear', list: '购物' })).toMatchObject({ removed: 3 });
  await voiceNotes(store, { action: 'drop', list: '购物' });
  expect(await voiceNotes(store, { action: 'lists' })).toMatchObject({ status: 'empty' });
});

it('keeps repeated reminder wall times across DST, missed days and weekends', () => {
  const startAt = Date.parse('2026-03-06T02:30:00-05:00');
  const schedule = {
    executeAt: startAt,
    startAt,
    recurrence: 'daily' as const,
    seriesId: 's',
    timeZone: 'America/New_York',
    type: 'reminder' as const,
  };
  const next = (now: string, recurrence: 'daily' | 'weekdays' | 'weekly') =>
    new Date(nextOccurrence({ ...schedule, recurrence }, Date.parse(now))!).toISOString();
  expect(next('2026-03-07T10:00:00-05:00', 'daily')).toBe('2026-03-08T07:30:00.000Z');
  expect(next('2026-03-08T10:00:00-04:00', 'daily')).toBe('2026-03-09T06:30:00.000Z');
  expect(next('2026-03-06T10:00:00-05:00', 'weekdays')).toBe('2026-03-09T06:30:00.000Z');
  expect(next('2026-06-12T10:00:00-04:00', 'weekly')).toBe('2026-06-19T06:30:00.000Z');
});

it('restores scheduled reminders, waits for playback, and cancels a recurring series without calling Being', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-06-01T12:00:00Z'));
  let saved: unknown;
  const speak = vi.fn(),
    request = vi.fn();
  const options = {
    scene: () => ({ scene_id: 'one' }) as ChatScene,
    emit: vi.fn(),
    request,
    speak,
    load: () => saved,
    save: (rows: unknown) => {
      saved = structuredClone(rows);
    },
  };
  let tasks = new VoiceTasks(options);
  const call = (name: string, args: VoiceToolCall['args']) =>
    tasks.run({ name, args, id: 'call', responseId: 'r', turnId: 'u', transcript: '安排提醒' });
  try {
    const args = { reminder: '起身活动', execute_at: '2026-06-01T12:00:01Z', recurrence: 'daily' };
    const receipt = (await call('schedule_reminder', args)) as {
      status: string;
      task_id: string;
      series_id: string;
    };
    expect(receipt.status).toBe('scheduled');
    expect(await call('schedule_reminder', args)).toMatchObject({
      status: 'already_submitted',
      task_id: receipt.task_id,
    });
    tasks.dispose();
    tasks = new VoiceTasks(options);
    await vi.advanceTimersByTimeAsync(1500);
    expect(speak).not.toHaveBeenCalled();
    tasks.resume();
    expect(speak).toHaveBeenCalledExactlyOnceWith('起身活动', [receipt.task_id]);
    tasks.playback({ type: 'playback.ended', taskIds: [receipt.task_id] });
    tasks.suspend();
    tasks.resume();
    expect(speak).toHaveBeenCalledTimes(1);
    expect(await call('get_agent_task_status', { list_all: true })).toMatchObject({
      active_count: 1,
      tasks: [{ status: 'scheduled' }],
    });
    expect(await call('cancel_agent_task', { series_id: receipt.series_id })).toMatchObject({
      status: 'cancelled',
    });
    await vi.advanceTimersByTimeAsync(2 * 86_400_000);
    expect(speak).toHaveBeenCalledTimes(1);
    expect(request).not.toHaveBeenCalled();
  } finally {
    tasks.dispose();
  }
});
