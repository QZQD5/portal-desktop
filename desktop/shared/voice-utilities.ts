export const sensitiveVoiceText =
  /pass(?:word)?|secret|api[_ -]?key|access[_ -]?token|credential|验证码|密码|密钥|令牌|\bsk-[a-z0-9_-]+/i;

interface NoteList {
  items: string[];
  updated_at: number;
}
export interface VoiceNotesStore {
  read(key: string): unknown;
  write(key: string, value: unknown): void;
}

function matches(values: string[], needle: string) {
  const lower = needle.toLowerCase();
  const exact = values.filter(value => value.toLowerCase() === lower);
  return exact.length
    ? exact
    : values.filter(value => value.toLowerCase().includes(lower) || lower.includes(value.toLowerCase()));
}

export async function voiceNotes(store: VoiceNotesStore, args: Record<string, unknown>) {
  const action = args.action;
  const clean = (value: string, limit: number) => value.trim().replace(/\s+/g, ' ').slice(0, limit);
  let name = typeof args.list === 'string' ? clean(args.list, 30) : '';
  if (!['lists', 'show', 'add', 'remove', 'clear', 'drop'].includes(String(action)))
    throw new Error('没有识别出要执行的清单操作。');
  if (action !== 'lists' && !name) throw new Error('需要明确要操作的清单名称。');
  const items = Array.isArray(args.items)
    ? args.items
        .filter((x): x is string => typeof x === 'string' && !!x.trim())
        .slice(0, 20)
        .map(x => clean(x, 100))
    : [];
  if ((action === 'add' || action === 'remove') && !items.length)
    throw new Error('需要明确要添加或划掉的内容。');
  if (items.some(text => sensitiveVoiceText.test(text)))
    throw new Error('不能保存密码、密钥、验证码或令牌。');
  const stored = store.read('notes_v2') ?? store.read('notes') ?? {};
  const lists = new Map<string, NoteList>();
  for (const [key, raw] of Object.entries(stored)) {
    const entry = Array.isArray(raw) ? { items: raw, updated_at: 0 } : (raw as NoteList);
    if (entry && Array.isArray(entry.items) && entry.items.every(x => typeof x === 'string')) {
      lists.set(key, { items: [...entry.items], updated_at: entry.updated_at || 0 });
    }
  }
  if (action === 'lists')
    return {
      status: lists.size ? 'ok' : 'empty',
      lists: Array.from(lists, ([list, entry]) => ({
        list,
        count: entry.items.length,
        updated_at: entry.updated_at,
      })).sort((a, b) => b.updated_at - a.updated_at),
    };
  const candidates = matches([...lists.keys()], name);
  if (candidates.length > 1) return { status: 'ambiguous', candidates: candidates.slice(0, 10) };
  if (candidates.length) name = candidates[0];
  else if (action !== 'add') return { status: 'not_found', candidates: [...lists.keys()].slice(0, 10) };
  else if (lists.size >= 20) return { status: 'list_full', list: name };
  const entry = lists.get(name) || { items: [], updated_at: Date.now() };
  if (action === 'show') {
    const rows = await Promise.all(
      entry.items.map(async text => {
        const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
        const hex = Array.from(new Uint8Array(digest, 0, 6), byte => byte.toString(16).padStart(2, '0')).join(
          '',
        );
        return { id: 'item_' + hex, text };
      }),
    );
    return { status: 'ok', list: name, items: rows };
  }
  const result: Record<string, unknown> = { status: 'ok', list: name };
  let changed = true;
  if (action === 'add') {
    const added: string[] = [],
      duplicates: string[] = [];
    for (const item of items) {
      if (entry.items.some(text => text.toLowerCase() === item.toLowerCase())) duplicates.push(item);
      else if (entry.items.length < 100) {
        entry.items.push(item);
        added.push(item);
      }
    }
    if (!added.length && !duplicates.length) return { status: 'list_full', list: name };
    Object.assign(result, { added, duplicates });
    changed = added.length > 0;
  } else if (action === 'remove') {
    const removed: string[] = [],
      missing: string[] = [],
      ambiguous: { text: string; candidates: string[] }[] = [];
    for (const item of items) {
      const found = matches(entry.items, item);
      if (found.length === 1) {
        entry.items.splice(entry.items.indexOf(found[0]), 1);
        removed.push(found[0]);
      } else if (found.length) ambiguous.push({ text: item, candidates: found.slice(0, 10) });
      else missing.push(item);
    }
    Object.assign(result, {
      status: ambiguous.length ? 'ambiguous' : removed.length ? 'ok' : 'not_found',
      removed,
      not_found: missing,
      ambiguous,
    });
    changed = removed.length > 0;
  } else if (action === 'clear') {
    result.removed = entry.items.length;
    changed = entry.items.length > 0;
    entry.items = [];
  } else lists.delete(name);
  if (changed) {
    if (action !== 'drop') {
      entry.updated_at = Date.now();
      lists.set(name, entry);
    }
    store.write('notes_v2', Object.fromEntries(lists));
  }
  return result;
}

export type Recurrence = 'once' | 'daily' | 'weekly' | 'weekdays';
export interface VoiceSchedule {
  executeAt: number;
  startAt: number;
  recurrence: Recurrence;
  seriesId: string;
  timeZone: string;
  type: 'task' | 'reminder';
}

/** Preserve the series' original wall clock, including DST changes and missed occurrences. */
export function nextOccurrence(schedule: VoiceSchedule, now: number) {
  if (schedule.recurrence === 'once') return;
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: schedule.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  const day = 86_400_000;
  const wall = (time: number) => {
    const fields = Object.fromEntries(
      formatter.formatToParts(time).map(part => [part.type, Number(part.value)]),
    );
    return Date.UTC(
      fields.year,
      fields.month - 1,
      fields.day,
      fields.hour,
      fields.minute,
      fields.second,
      time % 1000,
    );
  };
  const start = wall(schedule.startAt);
  const step = schedule.recurrence === 'weekly' ? 7 : 1;
  const elapsedDays = Math.floor(wall(now) / day) - Math.floor(start / day);
  for (let days = Math.max(step, step * Math.ceil(elapsedDays / step)); ; days += step) {
    const target = start + days * day;
    if (schedule.recurrence === 'weekdays' && [0, 6].includes(new Date(target).getUTCDay())) continue;
    // At a clock change, pick the first matching instant; move a skipped hour forward.
    const offsets = [target - day, target + day].map(time => wall(time) - time);
    const candidates = offsets.map(offset => target - offset);
    const exact = candidates.filter(time => wall(time) === target);
    const next = exact.length ? Math.min(...exact) : Math.max(...candidates);
    if (next > now) return next;
  }
}
