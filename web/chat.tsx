import { createRoot } from "react-dom/client";
import { useEffect, useMemo, useSyncExternalStore, type ComponentProps } from "react";
import type { Message } from "../desktop/renderer/chat/models/chat";
import { withVoiceHistory } from "../desktop/renderer/chat/models/voice-messages";
import { ChatApp, ChatView } from "../desktop/renderer/chat/page";
import { useModel } from "../desktop/renderer/shared/hooks/use-model";
import { useVoice } from "../desktop/renderer/voice/use-voice";
import { readConnection, deploymentConnection, type WebConnection } from "./connection";
import { loadDeployment } from "./deployment";
import { createWebVoice } from "./voice";
import "../desktop/renderer/chat/document.scss";
import "highlight.js/styles/github-dark.min.css";
import "./chat.scss";

const root = createRoot(document.getElementById("root")!);
const saved = readConnection();
const noRows: Message[] = [];
const emptyHistory = () => noRows;
const inactive = () => () => {};

function ForegroundChat({ connection, account, ...props }: ComponentProps<typeof ChatView> & { connection: WebConnection; account: string }) {
  const state = useModel(props.state);
  const { sceneId = "", sceneLabel = "" } = state.currentScene;
  const api = useMemo(() => {
    if (!sceneId) return undefined;
    const scene = { scene_id: sceneId, scene_meta: { client: "town-web", scene_label: sceneLabel || sceneId } };
    const history = (rows: readonly Message[]) => withVoiceHistory(state.items, rows).flatMap(item => {
      if (item.kind !== "message" || item.sceneId !== sceneId || item.role === "system" || item.queued || item.streaming || !item.text) return [];
      return [{ role: item.role === "user" ? "user" : "assistant", content: item.text }];
    }).slice(-20);
    return createWebVoice(connection, account, scene, history, () => props.runtime.activeTypedWork?.());
  }, [connection, account, sceneId, sceneLabel, state, props.runtime]);
  const voice = useVoice(api, sceneId, Boolean(api), sceneId, state.appendDictation);
  const history = useSyncExternalStore(api?.onEvent ?? inactive, api?.historyRows ?? emptyHistory);
  useEffect(() => () => api?.dispose(), [api]);
  useEffect(() => {
    const stop = () => voice.command("stop");
    const visibility = () => { if (document.hidden) stop(); };
    const leftChat = (event: MessageEvent) => {
      if (event.source === parent && event.origin === location.origin && event.data?.type === "beings:web-voice-stop") stop();
    };
    window.addEventListener("pagehide", stop);
    window.addEventListener("message", leftChat);
    document.addEventListener("visibilitychange", visibility);
    return () => {
      window.removeEventListener("pagehide", stop);
      window.removeEventListener("message", leftChat);
      document.removeEventListener("visibilitychange", visibility);
    };
  }, [voice.command]);
  return <ChatView {...props} webVoice={{...voice, history}}/>;
}

if (saved) {
  const connection = await deploymentConnection(saved);
  const deployment = await loadDeployment();
  // The browser needs the same-origin WebSocket upgrade endpoint to authenticate
  // voice. A static-only host continues to provide all existing text features.
  const voiceAvailable = deployment.mode === "server" && window.isSecureContext && !!navigator.mediaDevices?.getUserMedia;
  // Scope stored work to this credential even if another tab changes accounts.
  const identity = voiceAvailable && await crypto.subtle.digest("SHA-256",
    new TextEncoder().encode(connection.endpoint + "\0" + connection.token));
  const account = identity ? Array.from(new Uint8Array(identity), byte => byte.toString(16).padStart(2, "0")).join("") : "";
  const View: typeof ChatView = props => <ForegroundChat {...props} connection={connection} account={account}/>;
  root.render(
    <ChatApp connection={connection} keywordNavigation={false} mobileWeb View={voiceAvailable ? View : ChatView}/>,
  );
} else root.render(<p role="status">请先在网页版中连接你的 Being。</p>);
