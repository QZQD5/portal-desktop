import type { ChatScene } from "../desktop/shared/types";
import type { VoiceAPI, VoiceEvent } from "../desktop/shared/voice";
import { RealtimeVoice } from "../desktop/shared/voice-realtime";
import { VoiceTasks } from "../desktop/shared/voice-tasks";
import { VOICE_STORAGE_PREFIX, type WebConnection } from "./connection";
import { readCredential, writeCredential } from "./credential-storage";
import type { ChatRuntime, Message } from "../desktop/renderer/chat/models/chat";
import { HistoryCache } from "../desktop/renderer/chat/services/history-cache";

export function createWebVoice(connection: WebConnection, account: string, scene: ChatScene,
    history: (archived: readonly Message[]) => { role: string; content: string }[], textWork: NonNullable<ChatRuntime["activeTypedWork"]>) {
  const listeners = new Set<(event: VoiceEvent) => void>();
  const emit = (event: VoiceEvent) => { for (const listener of listeners) listener(event); };
  const cache = new HistoryCache(connection.endpoint);
  let archived: Message[] = [];
  const restored = cache.readVoice(account, scene.scene_id).then(rows => {
    archived = rows;
    emit({type: "web.voice.history"});
  }).catch(() => {});
  const prefix = VOICE_STORAGE_PREFIX + account + ":" + encodeURIComponent(scene.scene_id) + ":";
  const read = (key: string) => readCredential(prefix + key);
  const write = (key: string, value: unknown) => writeCredential(prefix + key, value);
  const request = (route: string, init: RequestInit) => {
    const headers = new Headers(init.headers);
    headers.set("Authorization", "Bearer " + connection.token);
    return fetch(connection.api + route, { ...init, headers, credentials: "omit", redirect: "error", cache: "no-store" });
  };
  const tasks = new VoiceTasks({
    scene: () => scene,
    history: () => history(archived), load: () => read("tasks"), save: value => write("tasks", value), read, write,
    request, textWork,
    async stopText(id, order) {
      const active = await request("/api/stream/active", {});
      if (!active.ok) return active;
      const stream = active.status === 204 ? null : await active.json();
      const target = textWork();
      if (!target || target.stream_id !== id || target.order !== order || target.scene_id !== scene.scene_id ||
          stream?.stream_id !== id || stream.scene_id !== scene.scene_id || stream.finished) return new Response(null, { status: 409 });
      return request("/api/stop", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ stream_id: id }) });
    },
    emit,
    speak: (text, ids) => realtime.speak(text, ids),
  });
  const realtime = new RealtimeVoice({
    connect(handlers) {
      const beingId = new URL(connection.endpoint).pathname.replace(/^\/+|\/+$/g, "");
      if (!/^[A-Za-z0-9_-]{1,128}$/.test(beingId)) throw new Error("当前 Being 链接不能用于语音通话。");
      const url = new URL("/v1/realtime/" + beingId, location.origin);
      url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
      const socket = new WebSocket(url);
      let connected = false;
      socket.onopen = () => socket.send(connection.token);
      socket.onmessage = event => {
        if (connected) { handlers.message(event.data); return; }
        if (event.data === '{"type":"web.voice.ready"}') { connected = true; handlers.open(); }
        else { handlers.error("语音代理握手失败。"); socket.close(); }
      };
      socket.onerror = () => handlers.error("语音服务连接失败，请检查网络连接。");
      socket.onclose = event => {
        if (!connected && event.code !== 1008 && event.reason) handlers.error(event.reason);
        handlers.close(event.code);
      };
      return { get bufferedAmount() { return socket.bufferedAmount; }, send: text => socket.send(text), close: () => socket.close() };
    },
    emit(event) {
      // Providers also transcribe announcements and read-aloud text that already
      // has a chat bubble. Archive only the user's turn and the model's own reply.
      if (event.type === "transcript.final" && typeof event.content === "string" && event.content.trim() &&
          (event.role === "user" || (event.role === "assistant" && event.origin === "model"))) {
        const user = event.role === "user";
        const id = user ? event.turnId : event.responseId;
        if (typeof id === "string" && id) {
          const voiceKey = (user ? "user:" : "voice:") + id;
          const index = archived.findIndex(row => row.voiceKey === voiceKey);
          const row: Message = { kind: "message", id: "web-voice:" + scene.scene_id + ":" + voiceKey,
            role: user ? "user" : "being", label: user ? "你" : "Being", sceneId: scene.scene_id,
            sceneLabel: scene.scene_meta?.scene_label, voiceKey, text: event.content, streaming: false,
            createdAt: archived[index]?.createdAt ?? Date.now(), timestamp: "", consecutive: false };
          archived = index < 0 ? [...archived, row] : archived.map((old, i) => i === index ? row : old);
          void cache.writeVoice(account, row).catch(() => emit({ type: "error", message: "语音记录未能保存在此设备。" }));
        }
      }
      if (event.type === "voice.ready") tasks.resume();
      if (["playback.requested", "playback.ended", "playback.cancelled"].includes(event.type)) tasks.playback(event);
      if (event.type === "desktop.voice" && event.state !== "ready") tasks.suspend();
      emit(event);
    },
    history: async () => { await restored; return history(archived); },
    context: () => ({ being_name: connection.name, scene }), captureTargets: () => tasks.cancellableIds(),
    tools: tasks.tools, runTool: call => tasks.run(call),
  });
  return {
    historyRows: () => archived,
    localActivity: true,
    start: () => realtime.start(),
    stop: async () => realtime.stop(),
    send: async event => realtime.send(event),
    onEvent(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
    dispose() { realtime.stop(); tasks.dispose(); cache.close(); listeners.clear(); },
  } satisfies VoiceAPI & { historyRows(): Message[]; dispose(): void };
}
