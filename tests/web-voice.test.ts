import { createServer } from "node:http";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { expect, it, vi } from "vitest";
import WebSocket, { WebSocketServer } from "ws";
import { installVoiceProxy } from "../web/voice-proxy.mjs";
import { createWebVoice } from "../web/voice";

it("keeps two simultaneous callers' audio and credentials on their own upstream sockets", async () => {
  const provider = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(provider, "listening");
  provider.on("connection", (socket, request) => {
    const being = request.url?.split("/").pop();
    socket.on("message", audio => {
      socket.send(JSON.stringify({ being, token: request.headers.authorization, audio: audio.toString() }));
    });
  });
  const server = createServer();
  installVoiceProxy(server, `ws://127.0.0.1:${(provider.address() as AddressInfo).port}/v1/realtime/`);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const callers = ["willow", "birch"].map(being => ({
    being, token: "fixture.voice-token-" + being,
    socket: new WebSocket(origin.replace("http:", "ws:") + "/v1/realtime/" + being, { origin }),
  }));
  try {
    await Promise.all(callers.map(async ({ token, socket }) => {
      await once(socket, "open");
      const ready = once(socket, "message");
      socket.send(token);
      expect(JSON.parse(String((await ready)[0])).type).toBe("web.voice.ready");
    }));
    for (let frame = 0; frame < 2; frame++) {
      await Promise.all(callers.map(async ({ being, token, socket }) => {
        const reply = once(socket, "message");
        const audio = being + "-pcm-" + frame;
        socket.send(audio);
        expect(JSON.parse(String((await reply)[0]))).toEqual({ being, token: "Bearer " + token, audio });
      }));
    }
  } finally {
    for (const { socket } of callers) socket.terminate();
    for (const socket of provider.clients) socket.terminate();
    await Promise.all([new Promise<void>(resolve => provider.close(() => resolve())),
      new Promise<void>(resolve => server.close(() => resolve()))]);
  }
});

it("authenticates a browser voice connection via its first frame and forwards realtime messages in both directions", async () => {
  const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(upstream, "listening");
  const server = createServer();
  installVoiceProxy(server, `ws://127.0.0.1:${(upstream.address() as AddressInfo).port}/v1/realtime/`);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const url = origin.replace("http:", "ws:") + "/v1/realtime/willow";
  const client = new WebSocket(url, { origin });
  try {
    const accepted = once(upstream, "connection");
    const ready = once(client, "message");
    await once(client, "open");
    client.send("test.voice-token-0001");
    const [peer, request] = await accepted;
    expect(request.url).toBe("/v1/realtime/willow");
    expect(request.headers.authorization).toBe("Bearer test.voice-token-0001");
    expect(JSON.parse((await ready)[0].toString())).toEqual({ type: "web.voice.ready" });
    const frame = once(peer, "message");
    client.send('{"type":"session.create"}');
    expect((await frame)[0].toString()).toBe('{"type":"session.create"}');
    const response = once(client, "message");
    peer.send('{"type":"session.created"}');
    expect((await response)[0].toString()).toBe('{"type":"session.created"}');
    const closed = once(peer, "close");
    client.close();
    await closed;
    const wrongOrigin = new WebSocket(url, { origin: "https://other.invalid" });
    expect(String((await once(wrongOrigin, "error"))[0])).toContain("403");
    const invalid = new WebSocket(url, { origin });
    await once(invalid, "open");
    const denied = once(invalid, "close");
    invalid.send("bad");
    expect((await denied)[0]).toBe(1008);
  } finally {
    client.terminate();
    for (const peer of upstream.clients) peer.terminate();
    await Promise.all([new Promise<void>(resolve => server.close(() => resolve())),
      new Promise<void>(resolve => upstream.close(() => resolve()))]);
  }
});

it("keeps a delayed voice stop bound to the original typed stream and submission", async () => {
  let deliver!: (event: object) => void;
  let result: { status: string } | undefined;
  vi.stubGlobal("location", new URL("http://localhost:4174"));
  vi.stubGlobal("WebSocket", class {
    bufferedAmount = 0;
    onopen?: () => void;
    onmessage?: (event: { data: string }) => void;
    constructor() {
      deliver = event => this.onmessage?.({ data: JSON.stringify(event) });
      queueMicrotask(() => this.onopen?.());
    }
    send(text: string) {
      if (text === "fixture.voice-token") { deliver({ type: "web.voice.ready" }); return; }
      const event = JSON.parse(text);
      if (event.type === "session.create") queueMicrotask(() => deliver({ type: "session.created" }));
      if (event.items?.[0].role === "tool") result = JSON.parse(event.items[0].content[0].text);
    }
    close() {}
  });
  let resolveActive!: (response: Response) => void, order = 1;
  const requests = vi.fn(() => new Promise<Response>(resolve => { resolveActive = resolve; }));
  vi.stubGlobal("fetch", requests);
  const voice = createWebVoice({ endpoint: "https://example.test/willow", api: "http://localhost:4174/loom-api",
    token: "fixture.voice-token", name: "willow", relaySecret: "" }, "account", {
    scene_id: "scene-a", scene_meta: { client: "town-web", scene_label: "Scene" },
  }, () => [], () => ({ stream_id: "original", scene_id: "scene-a", status: "running", order }));
  try {
    await voice.start();
    deliver({ type: "conversation.item.input_audio_transcription.completed", item_id: "turn", transcript: "停止" });
    deliver({ type: "response.function_call_arguments.done", call_id: "stop", name: "cancel_agent_task", arguments: {} });
    await vi.waitFor(() => expect(requests).toHaveBeenCalledTimes(1));
    order++;
    resolveActive(Response.json({ stream_id: "original", scene_id: "scene-a" }));
    await vi.waitFor(() => expect(result?.status).toBe("error"));
    expect(requests).toHaveBeenCalledTimes(1);
  } finally {
    voice.dispose();
    vi.unstubAllGlobals();
  }
});
