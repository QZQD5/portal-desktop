import type { ChatScene } from './types';
import { ChatEventDecoder } from './chat-sse';

type Data = Record<string, any>;
type ReplayItem = { seq: number; event: string; data: Data };
export interface ChildReceipt {
  subagentCalls?: Record<string, string>;
  subagents?: Record<string, string>;
}
const terminal = new Set(['done', 'failed', 'cancelled', 'interrupted', 'budget_exhausted', 'timeout']);
export const terminalChild = (status: string) => terminal.has(status);
export const pendingChildren = (receipt: ChildReceipt) =>
  Object.values(receipt.subagentCalls ?? {}).includes('spawn') ||
  Object.values(receipt.subagents ?? {}).some((status) => !terminalChild(status));

// Heart can return a JSON object, an MCP text envelope, or the CLI's prefixed JSON output.
function toolPayload(value: any): Data | undefined {
  for (let depth = 0; depth <= 4; depth++) {
    if (typeof value === 'string') {
      try {
        value = JSON.parse(value);
      } catch {
        const summary =
          /"summary"\s*:\s*"(sub_[\w-]{1,128}) (queued|running|done|failed|cancelled|interrupted|budget_exhausted|timeout)(?:"|$)/.exec(
            value,
          );
        if (summary) return { task_id: summary[1], status: summary[2] };
        const json = value.slice(value.indexOf('{'), value.lastIndexOf('}') + 1);
        try {
          value = JSON.parse(json);
        } catch {
          return;
        }
      }
    } else if (Array.isArray(value) || Array.isArray(value?.content)) {
      if (value.isError || value.is_error) return value;
      const texts = (Array.isArray(value) ? value : value.content).filter(
        (item: Data) => typeof item?.text === 'string',
      );
      value = texts.length === 1 ? texts[0].text : undefined;
    } else return value && typeof value === 'object' ? value : undefined;
  }
}

/** A text assertion cannot change child state. Match the result to the observed tool call. */
export function observeChildTool(receipt: ChildReceipt, event: string, data: Data): string[] {
  if (event !== 'tool_use' && event !== 'tool_result') return [];
  let id = [data.tool_call_id, data.tool_use_id, data.call_id, data.id].find(
    (value): value is string => typeof value === 'string' && value.length > 0 && value.length <= 256,
  );
  const calls = (receipt.subagentCalls ??= {});
  if (event === 'tool_use') {
    const operation =
      data.name === 'act'
        ? /^\s*portal_subagent_(spawn|status|control)\b(?!\s*\?)/.exec(
            toolPayload(data.input)?.body ?? '',
          )?.[1]
        : /(?:^|[:./_])portal_subagent_(spawn|status|control)$/.exec(data.name ?? '')?.[1];
    if (!id) {
      let index = 0;
      while (Object.hasOwn(calls, 'unidentified:' + index)) index++;
      id = 'unidentified:' + index;
    }
    Object.defineProperty(calls, id, {
      value: operation ?? 'other',
      enumerable: true,
      configurable: true,
      writable: true,
    });
    return [];
  }
  if (!id && new Set(Object.values(calls)).size === 1)
    id = Object.keys(calls).find((key) => key.startsWith('unidentified:'));
  if (!id || !Object.hasOwn(calls, id)) return [];
  const operation = calls[id];
  if (
    operation === 'other' ||
    data.is_error ||
    data.isError ||
    data.exit_code ||
    data.result?.is_error ||
    data.result?.isError ||
    data.result?.exit_code
  ) {
    delete calls[id];
    return [];
  }
  const value = [data.result, data.content, data.stdout, data.summary].map(toolPayload).find(Boolean);
  if (!value) return [];
  if (value.is_error || value.isError) {
    delete calls[id];
    return [];
  }
  const rows = operation === 'status' ? (value.tasks ?? [value.task ?? value]) : [value];
  const children = (receipt.subagents ??= {}),
    observed: string[] = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    const child = row?.task_id;
    if (
      typeof child !== 'string' ||
      !/^sub_[\w-]{1,128}$/.test(child) ||
      (operation !== 'spawn' && !Object.hasOwn(children, child))
    )
      continue;
    children[child] =
      terminal.has(row.status) || ['running', 'queued'].includes(row.status) ? row.status : 'unknown';
    observed.push(child);
  }
  if (operation !== 'spawn' || observed.length) delete calls[id];
  return observed;
}

/** Called with a complete callback reply, because its task marker can span deltas. */
export function callbackReply(items: ReplayItem[], sourceId?: string, children: Record<string, string> = {}) {
  let explicit: string | undefined;
  const texts: string[] = [],
    observed = new Set<string>();
  const receipt: ChildReceipt = { subagents: { ...children } };
  for (const { event, data } of items) {
    if (event === 'meta' && (data.continuation || 'task_id' in data)) {
      sourceId = explicit = typeof data.task_id === 'string' ? data.task_id : undefined;
    }
    if (event === 'content_block_delta' && typeof data.delta?.text === 'string') texts.push(data.delta.text);
    for (const id of observeChildTool(receipt, event, data)) observed.add(id);
  }
  const marker =
    /^\s*<!-- gateway-task: (task_[a-f0-9]{32}|[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}) -->[ \t]*(?:\r?\n|$)/.exec(
      texts.join(''),
    );
  const owner = explicit ?? marker?.[1] ?? sourceId;
  let remaining = marker?.[0].length ?? 0;
  const cleaned = items.map((item) => {
    const text = item.data.delta?.text;
    if (!remaining || item.event !== 'content_block_delta' || typeof text !== 'string') return item;
    const visible = text.slice(remaining);
    remaining = Math.max(0, remaining - text.length);
    return { ...item, data: { ...item.data, delta: { ...item.data.delta, text: visible } } };
  });
  return {
    items: cleaned,
    sourceId,
    ids: owner ? [owner] : [...observed],
    children: Object.fromEntries([...observed].map((id) => [id, receipt.subagents![id]])),
  };
}

/** Called after stopping the parent stream. Undefined represents verified completion. */
export async function cancelChildTasks(
  receipt: ChildReceipt,
  scene: ChatScene,
  post: (route: string, body: object) => Promise<Response>,
): Promise<Response | undefined> {
  if (!pendingChildren(receipt)) return;
  const unconfirmed = () =>
    Response.json({ error: '子代理停止尚未确认，请在 Being 聊天中核对。' }, { status: 409 });
  if (Object.values(receipt.subagentCalls ?? {}).includes('spawn')) return unconfirmed();
  const children = Object.entries(receipt.subagents ?? {})
    .filter(([, status]) => !terminal.has(status))
    .map(([id]) => id);
  const control: ChildReceipt = {
    subagents: Object.fromEntries(children.map((id) => [id, receipt.subagents![id]])),
  };
  const response = await post('/api/chat/stream', {
    ...scene,
    message:
      '用户要求取消原工作。只取消以下已有子任务，不创建新工作，不操作其他任务。逐项调用 portal_subagent_control(action=cancel, task_id=对应ID)。若已结束，请调用 portal_subagent_status 核实。必须返回实际工具回执。\n' +
      JSON.stringify(children),
  });
  if (!response.ok) return response;
  if (
    response.status !== 200 ||
    !response.body ||
    !response.headers.get('content-type')?.includes('text/event-stream')
  ) {
    await response.body?.cancel();
    return unconfirmed();
  }
  const reader = response.body.getReader(),
    decoder = new ChatEventDecoder();
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      for (const frame of decoder.push(chunk.value)) {
        if (frame.event !== 'tool_use' && frame.event !== 'tool_result') continue;
        const data = JSON.parse(frame.data);
        if (data && typeof data === 'object') observeChildTool(control, frame.event, data);
      }
    }
  } finally {
    reader.releaseLock();
    Object.assign(receipt.subagents!, control.subagents);
  }
  if (pendingChildren(receipt)) return unconfirmed();
}
