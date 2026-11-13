import { afterEach, expect, it, vi } from 'vitest';
import { GatewayHistory } from '../desktop/renderer/chat/services/gateway-history';
import { HistoryCache, historyCacheDatabaseName } from '../desktop/renderer/chat/services/history-cache';

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

it('imports legacy history before reads and never writes a second renderer history database', async () => {
  const endpoint = 'https://fixture.test/migration';
  vi.stubGlobal('indexedDB', { databases: async () => [{ name: historyCacheDatabaseName(endpoint) }] });
  vi.spyOn(HistoryCache.prototype, 'read').mockResolvedValue({ messages: [
    { seq: 99, role: 'user', content: '保留旧记录', scene_id: 'room' },
  ], lastSeq: 99 });
  const write = vi.spyOn(HistoryCache.prototype, 'write');
  const requests: string[] = [];
  const request = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const url = new URL(String(input));
    requests.push(url.pathname);
    expect(new Headers(init?.headers).get('X-Portal-Being-Endpoint')).toBe(endpoint);
    if (url.pathname.endsWith('/import')) {
      expect(JSON.parse(String(init?.body)).messages[0].seq).toBe(99);
      return Response.json({ ok: true });
    }
    return Response.json({ messages: [{ seq: 1, role: 'user', content: '保留旧记录', scene_id: 'room' }], lastSeq: 1 });
  });
  const history = new GatewayHistory(endpoint, request);
  expect((await history.read(100)).messages[0].seq).toBe(1);
  await history.write([{ seq: 1, role: 'user', content: '新消息' }], 1);
  expect(write).not.toHaveBeenCalled();
  expect(requests).toEqual(['/api/local-history/import', '/api/local-history']);
});

it('rejects malformed or non-advancing history instead of looping indefinitely', async () => {
  vi.stubGlobal('indexedDB', undefined);
  const malformed = new GatewayHistory('https://fixture.test/malformed', async () => Response.json({ wrong: true }));
  await expect(malformed.read()).rejects.toThrow('响应格式');
  const messages = Array.from({ length: 500 }, (_, i) => ({ seq: i + 1, role: 'user', content: String(i) }));
  const repeating = new GatewayHistory('https://fixture.test/repeating', async () => Response.json({ messages, lastSeq: 500 }));
  await expect(repeating.read()).rejects.toThrow('游标未前进');
});
