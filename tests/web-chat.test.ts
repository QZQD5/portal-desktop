import { afterEach, expect, it, vi } from "vitest";
import { ChatState } from "../desktop/renderer/chat/models/chat";
import { createChatRuntime } from "../desktop/renderer/chat/services/runtime";

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});
it("sends a Unicode browser scene in JSON without invalid HTTP headers or URL credentials", async () => {
  vi.useFakeTimers();
  vi.stubGlobal(
    "location",
    new URL(
      "https://web.test/chat.html?scene_id=" + encodeURIComponent("loom-柳树"),
    ),
  );
  vi.stubGlobal(
    "document",
    Object.assign(new EventTarget(), { visibilityState: "visible" }),
  );
  vi.stubGlobal("window", new EventTarget());
  vi.stubGlobal("navigator", { onLine: true });
  vi.stubGlobal("requestAnimationFrame", (fn: () => void) =>
    setTimeout(fn, 16),
  );
  vi.stubGlobal("cancelAnimationFrame", clearTimeout);
  const calls: Request[] = [];
  const encoder = new TextEncoder();
  let stream!: ReadableStreamDefaultController<Uint8Array>;
  vi.stubGlobal("fetch", async (url: string, init: RequestInit) => {
    // Constructing a real Request catches non-Latin-1 header values before I/O.
    const request = new Request(url, init);
    if (new URL(request.url).pathname.endsWith("/api/history")) return Response.json({ messages: [] });
    calls.push(request);
    return new Response(new ReadableStream<Uint8Array>({
      start(controller) {
        stream = controller;
        stream.enqueue(encoder.encode('event: meta\ndata: {"stream_id":"typed-stream"}\n\n'));
      },
    }), { headers: { "Content-Type": "text/event-stream" } });
  });
  const state = new ChatState();
  const runtime = createChatRuntime(state, {
    connection: { api: "https://web.test/loom-api", token: "session-token" },
  });
  try {
    expect(runtime.activeTypedWork?.()).toBeUndefined();
    const sent = runtime.send("你好", []);
    await vi.waitFor(() => expect(runtime.activeTypedWork?.()).toMatchObject({
      stream_id: "typed-stream", scene_id: "loom-柳树", status: "running", order: 1,
    }));
    stream.enqueue(encoder.encode('event: content_block_delta\ndata: {"delta":{"text":"收到"}}\n\nevent: message_stop\ndata: {}\n\n'));
    stream.close();
    await sent;
    await vi.waitFor(() => expect(runtime.activeTypedWork?.()).toBeUndefined());
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe(
      "https://web.test/loom-api/api/chat/stream?token=session-token",
    );
    expect(calls[0].headers.has("X-Portal-Scene-Id")).toBe(false);
    expect(await calls[0].json()).toMatchObject({
      scene_id: "loom-柳树",
      message: "你好",
    });
    expect(location.search).not.toContain("session-token");
  } finally {
    runtime.dispose();
  }
});
