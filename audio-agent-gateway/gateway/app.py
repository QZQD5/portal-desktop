import asyncio
import contextlib
import hashlib
import json
import logging
import math
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response, StreamingResponse

from . import protocol
from .access import authorized
from .chat import Chat, register_chat
from .client_events import ClientEvents
from .config import desktop_display_name
from .content import InputAssets, embedded, normalize_parts
from .conversation_context import snapshot
from .errors import GatewayError
from .local_history import LocalHistory
from .logging import configure
from .retention import public_task
from .seeduplex import MODEL_PROFILE, voice_configured
from .store import Store, iso_now, now_ms, uid
from .tasks import TERMINAL, TaskManager
from .tools import Tools
from .transport import install_windows_cleanup, within_budget
from .voice import VoiceSession

log = logging.getLogger("gateway")
CAPABILITIES = {
    "input.text",
    "input.audio",
    "input.file",
    "input.image",
    "playback.receipts",
    "tasks.commands",
    "conversation.history",
    "session.replay",
    "session.heartbeat",
    "session.takeover",
    "session.output_voice",
    "client.events",
    "client.presence",
    "client.tools",
    "client.actions.desktop.presence.enter_sleep",
}
PROTOCOL = "7.0.0"


class Gateway:
    def __init__(self, settings):
        self.settings = settings
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        self.store = Store(settings.state / "gateway.sqlite3")
        self.history = LocalHistory(settings.data_dir / "history.sqlite3")
        for entry in settings.legacy_history:
            self.history.import_gateway(entry["path"], entry["scene"])
        self.store.attach_history(self.history, settings.being_scene)
        self.tasks = TaskManager(settings, self.store, self.broadcast)
        self.tools = Tools(settings, self.store, self.tasks)
        self.chat = Chat(self.tasks.client, self.history, self.interrupt_voice_for_text, self.sync_text_context,
                          self.tasks)
        self.chat.gateway = self
        self.input_assets = InputAssets(settings.session_ttl, settings.max_sessions)
        self.client_events = ClientEvents(settings.client_event_definitions)
        self.sessions = {}
        self.subscribers = set()
        self.active = None
        self.town_display_name = None
        self.started_at, self.instance = iso_now(), settings.instance_id or uid("gateway")
        self.holders = {}
        self.status_job = None
        self.status_lock = asyncio.Lock()
        self.closing = False
        self.expiry = None
        self.ownership_lock = asyncio.Lock()
        self.lease_generation = 0
        self.input_runtime = self

    @property
    def being_name(self):
        return self.town_display_name or self.tasks.client.name

    def set_desktop_display_name(self, value):
        name = desktop_display_name(value)
        if name != self.town_display_name:
            self.town_display_name = name
            if self.active and not self.active.closed:
                self.active.refresh_desktop_identity()

    async def interrupt_voice_for_text(self, scene_id, request_id):
        session = self.active
        same_scene = session and not session.closed and scene_id == self.settings.being_scene.get("scene_id")
        cancel_targets = self.tools.cancel_targets("text:" + scene_id, scene_id)
        if same_scene and not session.text_only:
            async with session.input_lock:
                same_scene = not session.closed and self.active is session and scene_id == self.settings.being_scene.get("scene_id")
                if same_scene and not session.text_only:
                    session.manual_generation = session.generation + 1
                    try:
                        await session.interrupt(reason="text_input")
                        session.committed_generation = session.generation
                        for record in session.input_records.values():
                            if record["generation"] < session.generation:
                                if record.get("started") and not record["invalid"] and not record["complete"]:
                                    record["context_only"] = True
                                record["invalid"] = True
                    finally:
                        session.manual_generation = None
        context = snapshot(self.store, self.tasks.tasks, session.id if same_scene else "text:" + scene_id, scene_id=scene_id)
        context["currentRequest"] = {"requestId": request_id, **({"turnId": session.turn} if same_scene else {})}
        context["cancelTargets"] = cancel_targets
        return self.tasks.client.context.capture(context)

    async def sync_text_context(self, scene_id, feedback=None):
        session = self.active
        if session and not session.closed and (scene_id is None or scene_id == self.settings.being_scene.get("scene_id")):
            if feedback and not session.text_only and session.output_enabled:
                session.offer_text_feedback(feedback)
            with contextlib.suppress(Exception):
                await session.sync_context()

    async def claim(self, ws, hello, instance, requested, *, notify=True):
        async with self.ownership_lock:
            session_id = ws.query_params.get("sessionId") or uid("session")
            if len(session_id) > 128:
                raise ValueError("sessionId is too long")
            hello = {**hello, "capabilities": sorted(set(requested) & CAPABILITIES)}
            session = VoiceSession(self, ws, session_id, hello)
            old = self.active if self.active and not self.active.closed else None
            replaced = bool(old)
            if old:
                takeover = hello.get("connection", {}).get("takeover") and "session.takeover" in requested
                if old.hello["client"]["instance_id"] != instance and not takeover:
                    await ws.close(code=4002, reason="Voice session is occupied")
                    return None
                await old.close()
                if old.ws is not ws:
                    with contextlib.suppress(Exception):
                        await old.ws.close(code=4001)
            self.active = session
            if hello["client"]["type"] == "desktop" and "being_name" in hello:
                self.set_desktop_display_name(hello["being_name"])
            self.sessions[instance] = session
            self.lease_generation += 1
            if not hello.get("legacy") and notify:
                await self.send(
                    session,
                    {
                        "type": "session.ready",
                        "request_event_id": hello["event_id"],
                        "protocol_version": PROTOCOL,
                        "session_id": session_id,
                        "capabilities": sorted(set(requested) & CAPABILITIES),
                        "connection": {"lease_generation": self.lease_generation, "replaced": replaced},
                    },
                )
            await session.start()
            return session

    @property
    def suspended(self):
        return bool(self.input_status()["suspended"])

    def input_status(self):
        if self.input_runtime is not self:
            return self.input_runtime.input_status()
        holders = [holder for holder in self.holders.values() if holder["expiresAt"] > now_ms()]
        return {
            "suspended": bool(holders),
            "holders": holders,
            "owner": holders[0]["owner"] if holders else None,
            "reason": holders[0]["reason"] if holders else "",
            "expiresAt": max((x["expiresAt"] for x in holders), default=None),
        }

    async def start(self, *, backend=True):
        self.chat.start()
        await self.tools.external.start()
        await self.tasks.start()
        if backend:
            self.status_job = asyncio.create_task(self.refresh_status())
        log.info("gateway.started instance=%s", self.instance)

    async def refresh_status(self, *, timeout=15):
        if self.closing or not self.settings.backend_enabled:
            return
        client = self.tasks.client
        try:
            async with asyncio.timeout(timeout), self.status_lock:
                old_name = client.name
                await client.start()
                name = client.name
        except Exception as exc:
            client.error = type(exc).__name__
            log.warning("backend.status_refresh_failed error=%s", client.error)
        else:
            session = self.active
            if name != old_name and session and not session.closed and session.ready.is_set():
                with contextlib.suppress(Exception):
                    await session.update_session()
            return name

    async def send(self, session, event):
        if session.closed or session.client_send_failed:
            return
        # Snapshots and command replies are derived views; only task changes need replay.
        durable = (
            event["type"].startswith("task.")
            and event["type"] not in {"task.snapshot", "task.output.delta"}
            and not event["type"].endswith(".result")
        ) or event["type"] == "transcript.final"
        payload = (
            self.store.event(session.id, event)
            if durable
            else {"event_id": uid(), "occurred_at": now_ms(), **event}
        )
        payload = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        size = len(payload.encode())
        if not within_budget(size, session.client_pending_bytes, audio=event["type"] == "audio.delta"):
            await self.fail_client_send(session)
            return
        session.client_pending_bytes += size
        sender = asyncio.create_task(self.send_client_payload(session, payload, event))
        session.client_send_tasks.add(sender)

        def completed(task):
            session.client_send_tasks.discard(task)
            session.client_pending_bytes -= size

        sender.add_done_callback(completed)
        await asyncio.sleep(0)

    def discarded_task_event(self, event):
        task = event.get("task") or {}
        if not self.tasks.output_discarded(event.get("taskId") or task.get("id")):
            return False
        return (event["type"] in {"task.updated", "task.activity", "task.output.delta"}
                or (event["type"] in {"task.completed", "task.failed", "task.notification.delivered"}
                    and not task.get("discard_output")))

    async def send_client_payload(self, session, payload, event):
        try:
            async with session.client_send_lock:
                if session.client_send_failed or self.discarded_task_event(event):
                    return
                if event.get("role") == "assistant" and any(
                    self.tasks.output_discarded(task_id) for task_id in (event.get("taskIds") or ())
                ):
                    return
                rid = event.get("responseId")
                if rid and (event["type"] in {"audio.delta", "audio.done", "response.started", "tool.call"}
                            or event.get("role") == "assistant"):
                    if session.discarded_response(session.responses.get(rid, {})):
                        return
                await session.ws.send_text(payload)
        except (WebSocketDisconnect, RuntimeError, OSError):
            log.info("websocket.send_failed session=%s", session.id)
            await self.fail_client_send(session)

    async def fail_client_send(self, session):
        if session.client_send_failed:
            return
        session.client_send_failed = True
        with contextlib.suppress(WebSocketDisconnect, RuntimeError, OSError, TimeoutError):
            await asyncio.wait_for(
                session.ws.close(code=1013, reason="WebSocket send failed or backpressure"), 1
            )
        senders = [task for task in session.client_send_tasks if task is not asyncio.current_task()]
        for task in senders:
            task.cancel()
        await asyncio.gather(*senders, return_exceptions=True)

    async def broadcast(self, session_id, event):
        task = event.get("task")
        if event["type"] == "task.output.discarded" and task:
            self.chat.discard_finished_task(task["id"])
        for session in list(self.sessions.values()):
            if self.discarded_task_event(event):
                return
            if task and task.get("discard_output"):
                with contextlib.suppress(Exception):
                    await session.discard_cancelled_replies()
            if event["type"] == "task.output.delta" and event.get("chatStreamId"):
                continue  # The HTTP chat stream already delivers these deltas.
            if event["type"] == "task.output.delta" and task and task.get("sessionId") == session.id:
                with contextlib.suppress(Exception):
                    await session.speak_stream(task, event.get("content") or "")
            await self.send(session, event)
            if task and task.get("sessionId") == session.id:
                with contextlib.suppress(Exception):
                    await session.sync_context()
            if event["type"] in {
                "task.permission.requested",
                "task.input.requested",
                "task.permission.resolved",
                "task.input.resolved",
            }:
                with contextlib.suppress(Exception):
                    if event["type"].endswith(".resolved"):
                        request = event.get("permission") or event.get("input")
                        if request:
                            await session.resolve_request(request["id"], permission="permission" in event)
                    await session.update_session()
        if self.discarded_task_event(event):
            return
        if not any(session.id == session_id and not session.closed for session in self.sessions.values()):
            self.store.event(session_id, event)
        for queue in list(self.subscribers):
            if getattr(queue, "task_id", None) and queue.task_id != (event.get("taskId") or event.get("task", {}).get("id")):
                continue
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(event)

    def health(self):
        backend = self.tasks.client
        search = self.settings.web_search_provider
        search_configured = search in {"bing", "so360"} or (
            search in {"mcp", "bailian"}
            and bool(self.settings.web_search_mcp_url)
            and (search != "bailian" or bool(self.settings.web_search_mcp_token))
        )
        configured = voice_configured(self.settings)
        signature = hashlib.sha256(
            json.dumps(
                {
                    "provider": "doubao-seeduplex",
                    "endpoint": self.settings.doubao_url.rstrip("/?"),
                    "model": self.settings.doubao_model,
                    "voice": self.settings.doubao_voice,
                    "auth": "loom" if self.settings.being_token else "none",
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return {
            "ok": True,
            "status": "ready",
            "protocolVersion": "5.9.0",
            "capabilities": [
                "web.same-origin-ui",
                *(["gateway.instance-lease"] if self.settings.instance_id else []),
                "input.suspend-protocol",
                "input.suspend-clears-playback",
                "input.suspend-ttl",
                "input.suspend-ack",
                "tasks.ag-ui-event-stream",
                "tasks.structured-results-authorization",
                "tasks.unified-id-updates",
                "messages.citations",
                "realtime.conversation-client-v1",
                "realtime.visual-input-buffer-v1",
                "realtime.gateway-client-protocol-v6-handshake",
                "realtime.gateway-client-protocol-v6-runtime-commands",
                "realtime.gateway-client-protocol-v6-agent-delivery",
                "realtime.gateway-client-protocol-v6-client-actions",
                "realtime.gateway-client-protocol-v6-reference-client-replay",
                "gateway.remote-access-pairing",
                "gateway.direct-device-connection",
                "realtime.gateway-client-protocol-v6-owner-takeover",
            ],
            "gatewayInstanceId": self.instance,
            "gatewayStartedAt": self.started_at,
            "voiceConfigured": configured,
            "realtimeProvider": "doubao-seeduplex",
            "realtimeLabel": "Doubao Seeduplex Realtime",
            "realtimeModel": self.settings.doubao_model,
            "realtimeInputSampleRate": 16000,
            "realtimeProviders": [
                {
                    "key": "doubao-seeduplex",
                    "label": "Doubao Seeduplex Realtime",
                    "model": self.settings.doubao_model,
                    "realtimeModelIds": [self.settings.doubao_model],
                    "configured": configured,
                }
            ],
            "realtimeModelProfile": MODEL_PROFILE,
            "realtimeModelCatalog": [MODEL_PROFILE],
            "realtimeConfigurationSignature": signature,
            "announceIntoContext": self.settings.announce_into_context,
            "resultContextMaxChars": self.settings.result_context_max_chars,
            "announcementBatchMs": self.settings.announcement_batch_window * 1000,
            "announcementQuietMs": self.settings.announcement_quiet * 1000,
            "frontendMcp": self.tools.external.health("mcp"),
            "frontendOpenApi": self.tools.external.health("openapi"),
            "frontendRetrieval": {
                "capabilities": (["web-search"] if search_configured else []) + ["url-fetch"],
                "searchProvider": None
                if search == "none"
                else {
                    "key": "bailian-mcp" if search == "bailian" else search,
                    "label": {
                        "so360": "360 Web Search (fallback)",
                        "bing": "Bing Web Search (fallback)",
                        "mcp": "MCP Web Search",
                        "bailian": "Bailian Web Search MCP",
                    }[search],
                    **({"tool": self.settings.web_search_mcp_tool} if search in {"mcp", "bailian"} else {}),
                },
            },
            "notes": {
                "ok": True,
                "persistenceEnabled": True,
                "warning": None,
                "owners": int(bool(self.store.get("notes_v2") or self.store.get("notes"))),
            },
            "taskStore": {"ok": True, "persistenceEnabled": True, "warning": None},
            "publicEndpoint": {"mode": "none", "state": "disabled", "endpoint": None},
            "inputSuspension": self.input_status(),
            "voiceClients": {
                "connected": len(self.sessions),
                "activeOwners": int(bool(self.active)),
                "byType": {"web": len(self.sessions)},
            },
            "backend": {
                "kind": "being",
                "protocol": "heart",
                "label": self.tasks.session_model if backend.ready and self.tasks.session_model else "Being",
                "enabled": self.settings.backend_enabled,
                "ok": backend.ready,
                "status": "disabled"
                if not self.settings.backend_enabled
                else "ready"
                if backend.ready
                else "failed"
                if backend.error
                else "starting",
                "error": backend.error,
                "transport": "http-sse",
                "baseUrl": None,
                "uiPath": None,

                "backendAgent": None,
                "sessionModel": "desktop-being-scene",
                "capabilities": {
                    "delegation": False,
                    "permissions": False,
                    "backendUi": False,
                    "nativeSessionHistory": False,
                    "externalMcp": False,
                    "nativeDelegation": False,
                    "sessionMcp": False,
                    "coordinatorMcpInstructions": False,
                    "taskUpdates": "activity",
                    "inputRequests": False,
                },
                "ownership": "external",
                "model": self.tasks.session_model if backend.ready else None,
                "directory": str(self.settings.workspace),
                "agentInfo": (backend.info or {}).get("agentInfo"),
                **(backend.status() if self.settings.backend_enabled else {"status": "not_configured"}),
            },
        }

    async def command(self, session, event):
        if session.hello.get("legacy"):
            event = {"event_id": uid(), **event}
        protocol.event(event)
        kind = event["type"]
        required = {
            "task.create": "tasks.commands",
            "task.get": "tasks.commands",
            "task.list": "tasks.commands",
            "task.cancel": "tasks.commands",
            "permission.respond": "permissions.respond",
            "task.input.respond": "tasks.input.respond",
            "conversation.history": "conversation.history",
            "session.replay": "session.replay",
            "session.output_voice.update": "session.output_voice",
            "client.event.publish": "client.events",
            "client.presence.update": "client.presence",
            "input_image_buffer.append": "input.image_buffer",
            "input_image_buffer.clear": "input.image_buffer",
            "image.append": "input.image_buffer",
        }.get(kind)
        if required and required not in session.hello["capabilities"]:
            raise GatewayError("capability_not_negotiated", f"Capability not negotiated: {required}")
        if kind in {"input.message", "text.message", "conversation.item.create"}:
            parts = event.get("parts") or (
                [{"type": "text", "text": event["text"]}] if event.get("text") else None
            )
            await session.input(parts, request_id=event["event_id"])
        elif kind in {"audio.append", "input_audio_buffer.append"}:
            if "being_name" in event and session.hello.get("client", {}).get("type") == "desktop":
                self.set_desktop_display_name(event["being_name"])
            await session.append_audio(event.get("audio"))
        elif kind in {"image.append", "input_image_buffer.append"}:
            if (
                session.output_enabled
                and not session.sleeping
                and not self.suspended
                and session.voice_arbitration
            ):
                session.visual.append(event)
        elif kind in {"image.clear", "input_image_buffer.clear"}:
            session.visual.reset()
        elif kind in {"interrupt", "response.cancel"}:
            await session.interrupt()
            await session.state("idle")
        elif kind in {"playback.ended", "playback.cancelled"}:
            await session.playback_ended(
                event["responseId"], kind == "playback.cancelled", event.get("reason", "")
            )
        elif kind == "playback.started":
            await session.playback_started(event["responseId"])
        elif kind in {"input.unmute", "input.mute", "unmute", "mute"}:
            if kind == "mute":
                await session.mute()
            elif kind in {"unmute", "input.unmute"}:
                await session.unmute(input_only=kind == "input.unmute")
            else:
                session.input_enabled = False
                session.speaking_user = False
                await session.sync_input_state()
                await session.clear_audio()
                await session.state(
                    "speaking" if session.playing else
                    "processing" if session.active or session.native_pending_since else "idle"
                )
        elif kind == "client.action.result":
            session.actions.receive(event)
        elif kind == "sleep":
            # Do not block the socket reader: it must receive the action acknowledgement.
            async def sleep_request():
                try:
                    await session.request_sleep()
                except GatewayError as exc:
                    await session.emit(
                        {
                            "type": "error",
                            "message": str(exc),
                            "error": {"code": exc.code, "message": str(exc)},
                        }
                    )

            if not session.sleep_request or session.sleep_request.done():
                session.sleep_request = asyncio.create_task(sleep_request())
                session.background.append(session.sleep_request)
        elif kind in {"wake", "client.presence.update"}:
            await session.set_sleeping(event.get("state") == "sleeping")
        elif kind == "session.output_voice.update":
            session.output_voice = event.get("voice") or None
            await session.update_session()
            return {"voice": session.output_voice or self.settings.doubao_voice}
        elif kind == "conversation.history":
            return {"messages": self.store.history(event.get("session_id") or session.id)}
        elif kind == "session.replay":
            return self.store.replay(
                session.id,
                max(0, int(event.get("after_sequence", 0))),
                max(1, min(200, int(event.get("limit", 50)))),
            )
        elif kind == "task.list":
            return {
                "tasks": self.tasks.list(
                    event.get("session_id") or session.id,
                    event.get("active", False),
                    event.get("limit", 50),
                )
            }
        elif kind == "task.get":
            return {"task": public_task(self.tasks.tasks[event["task_id"]])}
        elif kind == "task.create":
            parts = normalize_parts(event.get("message", {}).get("parts", []))
            task = await self.tasks.submit(
                "\n".join(x["text"] for x in parts if x["type"] == "text") or "处理用户提供的附件",
                session.id,
                session.turn,
                submission_key=f"client:{session.id}:{event['event_id']}",
                parts=parts,
            )
            return {"task": public_task(task)}
        elif kind == "task.cancel":
            if self.tasks.tasks[event["task_id"]]["status"] in {"completed", "failed", "cancelled"}:
                raise GatewayError("task_not_cancellable", "task is no longer active", 409)
            operation = self.tasks.start_cancel(event["task_id"])
            # Being cancellation continues independently so this socket can accept new input.
            await asyncio.sleep(0)
            if operation.done():
                operation.result()
            return {"task": public_task(self.tasks.tasks[event["task_id"]])}
        elif kind == "permission.respond":
            return {
                "permission": await self.tasks.respond_permission(event["permission_id"], event["decision"])
            }
        elif kind == "task.input.respond":
            return {
                "input": await self.tasks.respond_input(
                    event["task_id"],
                    event["input_request_id"],
                    event["action"],
                    event.get("text", ""),
                    event.get("values"),
                )
            }
        elif kind == "client.event.publish":
            return await self.client_events.publish(session, event)
        elif kind == "session.pong":
            session.last_pong = time.monotonic()
        elif kind == "input.suspend.ack":
            log.debug("input.suspend_acknowledged session=%s owner=%s", session.id, event.get("owner"))
        else:
            raise ValueError(f"Unsupported event: {kind}")

    async def close(self):
        log.info("gateway.stopping instance=%s", self.instance)
        self.closing = True
        # Unwind every resource even when an earlier cleanup raises.
        async with contextlib.AsyncExitStack() as cleanup:
            cleanup.callback(self.history.close)
            cleanup.callback(self.store.close)
            cleanup.push_async_callback(self.tools.external.close)
            cleanup.push_async_callback(self.tasks.close)
            cleanup.push_async_callback(self.chat.close)
            for session in list(self.sessions.values()):
                cleanup.push_async_callback(session.close)
            jobs = [task for task in (self.expiry, self.status_job) if task]
            for task in jobs:
                task.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)
        log.info("gateway.stopped instance=%s", self.instance)


async def body_object(request):
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        return {}
    try:
        data = await request.json()
    except ValueError as exc:
        raise GatewayError("invalid_request", "Expected a JSON object") from exc
    if not isinstance(data, dict):
        raise GatewayError("invalid_request", "Expected a JSON object")
    return data


def create_app(settings=None):
    install_windows_cleanup()
    if settings is None:
        raise ValueError("Start Gateway through Portal Desktop")
    configure(settings.data_dir / "logs")

    @asynccontextmanager
    async def lifespan(app):
        gateway = app.state.gateway = Gateway(settings)
        try:
            await gateway.start()
            yield
        finally:
            await gateway.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    register_chat(app)

    @app.middleware("http")
    async def authenticated_access(request, call_next):
        if not authorized(request, settings):
            return JSONResponse({"error": "Unauthorized"}, 401)
        request.state.gateway = app.state.gateway
        if request.headers.get("content-type", "").split(";")[0].strip().lower() == "application/json":
            body = bytearray()
            limit = 64 * 1024 * 1024 if request.url.path == "/api/heart/chat/stream" else 4 * 1024 * 1024 if request.url.path == "/api/local-history/import" else 1024 * 1024
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > limit:
                    return JSONResponse({"error": "request entity too large"}, 413)
            request._body = bytes(body)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        if request.url.path.startswith("/api/") or response.headers.get("content-type", "").startswith("text/html"):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(ValueError)
    async def value_error(request, exc):
        return JSONResponse(
            {"error": str(exc), **({"code": exc.code} if isinstance(exc, GatewayError) else {})},
            getattr(exc, "status", 400),
        )

    @app.exception_handler(KeyError)
    async def key_error(request, exc):
        return JSONResponse({"error": "Not found"}, 404)

    @app.get("/livez")
    async def live():
        return {"ok": True, "status": "live"}

    @app.get("/readyz")
    async def ready():
        return {"ok": True, "status": "ready"}

    @app.get("/api/health")
    async def health(request: Request):
        result = request.state.gateway.health()
        runtimes = [app.state.gateway]
        sessions = [session for runtime in runtimes for session in runtime.sessions.values()]
        counts = dict.fromkeys(
            ("connected", "connecting", "disconnected", "unavailable", "sleeping", "waking"), 0
        )
        providers = {}
        for session in sessions:
            state = "sleeping" if session.sleeping else session.connection_state
            counts[state] = counts.get(state, 0) + 1
            provider = providers.setdefault("doubao-seeduplex", dict.fromkeys(counts, 0))
            provider[state] += 1
            if session.connection_error:
                provider["error"] = session.connection_error
        result["voiceClients"] = {
            "connected": len(sessions),
            "activeOwners": sum(bool(x.active) for x in runtimes),
            "activeClients": sum(bool(x.active) for x in runtimes),
            "byType": {
                kind: sum(s.hello["client"]["type"] == kind for s in sessions)
                for kind in {"desktop", "cli", "web"} | {s.hello["client"]["type"] for s in sessions}
            },
            "realtime": {**counts, "byProvider": providers},
        }
        return result


    @app.get("/api/tasks")
    async def task_list(
        request: Request, active: bool = False, limit: int | None = None, sessionId: str | None = None
    ):
        return {
            "tasks": request.state.gateway.tasks.list(
                sessionId, active, max(1, limit) if limit is not None else None
            )
        }

    @app.get("/api/tasks/{task_id}")
    async def task_get(task_id: str, request: Request):
        return public_task(request.state.gateway.tasks.tasks[task_id], detail=True)

    @app.delete("/api/tasks/{task_id}")
    async def task_cancel(task_id: str, request: Request):
        task = request.state.gateway.tasks.tasks[task_id]
        if task["status"] in {"completed", "failed", "cancelled"}:
            return JSONResponse({"error": "task is no longer active", "task": public_task(task)}, 409)
        task = await request.state.gateway.tasks.cancel(task_id)
        async with asyncio.timeout(60):
            while task["status"] == "cancelling":
                await asyncio.sleep(0.05)
        return public_task(task)


    @app.get("/api/conversations")
    async def conversations(request: Request):
        return {"conversations": request.state.gateway.store.conversations()}

    @app.get("/api/conversations/{session_id}/messages")
    async def messages(session_id: str, request: Request):
        return {"messages": request.state.gateway.store.history(session_id)}

    @app.get("/api/conversations/{session_id}/messages/{message_id}/inputs/{index}")
    async def message_input(session_id: str, message_id: str, index: int, request: Request):
        if not 0 <= index < 16:
            raise KeyError("Message attachment not found")
        part = request.state.gateway.store.message_input(session_id, message_id, index)
        data = embedded(part["url"])
        if not data or data[0] != part["mime"] or not data[0].startswith("image/"):
            raise KeyError("Message image not found")
        return Response(
            data[2], media_type=data[0],
            headers={
                "Cache-Control": "private, no-store",
                "X-Content-Type-Options": "nosniff",
                "Content-Security-Policy": "default-src 'none'; sandbox",
            },
        )

    @app.delete("/api/conversations/{session_id}")
    async def delete_conversation(session_id: str, request: Request):
        gateway = request.state.gateway
        tasks = [task for task in gateway.tasks.tasks.values() if task["sessionId"] == session_id]
        if any(task["status"] not in TERMINAL for task in tasks):
            raise GatewayError("conversation_busy", "请先取消或等待该会话的后台任务结束，再删除会话。", 409)
        for task in tasks:
            gateway.tasks.tasks.pop(task["id"])
            gateway.store.delete("task_context:" + task["id"])
        gateway.tasks.persist()
        gateway.store.delete_conversation(session_id)
        return {"ok": True}

    @app.get("/api/sessions/{session_id}/replay")
    async def replay(session_id: str, request: Request):
        gateway = request.state.gateway
        return {"replay": gateway.store.session_replay(session_id)}

    @app.get("/api/sessions/{session_id}/events")
    async def session_events(session_id: str, request: Request):
        return {"events": request.state.gateway.store.journal(session_id)}

    @app.get("/api/timeline")
    async def timeline(request: Request, sessionId: str | None = None):
        gateway = request.state.gateway
        return {
            "items": [
                {
                    "id": "inline_" + task["id"],
                    "taskId": task["id"],
                    "createdAt": task.get("completedAt") or task["createdAt"],
                    **task["presentation"]["inline"],
                }
                for task in sorted(
                    gateway.tasks.list(sessionId, limit=None),
                    key=lambda task: task.get("completedAt") or task["createdAt"],
                )
                if task.get("presentation", {}).get("inline", {}).get("content")
            ]
        }

    @app.get("/api/notes")
    async def notes(request: Request):
        return request.state.gateway.tools.notes({"action": "lists"})


    @app.get("/api/input")
    async def input_status(request: Request):
        return request.state.gateway.input_status()

    @app.post("/api/input/suspend")
    async def suspend(request: Request):
        data = await body_object(request)
        # This API arbitrates the host microphone, globally, as in upstream.
        # Its body.owner is a holder name, not an authenticated user identity.
        gateway = app.state.gateway
        owner = str(data.get("owner") or "").strip()[:80]
        if not owner:
            raise GatewayError("QWAUDIO_INPUT_OWNER_REQUIRED", "input.suspend requires owner")
        was_suspended = gateway.suspended
        try:
            ttl = float(data.get("ttlMs") or 15000)
        except (TypeError, ValueError):
            ttl = 15000
        ttl = min(300000, ttl) if math.isfinite(ttl) and ttl > 0 else 15000
        gateway.holders[owner] = {
            "owner": owner,
            "reason": str(data.get("reason", ""))[:200],
            "ttlMs": ttl,
            "since": gateway.holders.get(owner, {}).get("since", now_ms()),
            "expiresAt": now_ms() + ttl,
        }
        if not was_suspended:
            for runtime in [app.state.gateway]:
                for session in list(runtime.sessions.values()):
                    await session.sync_input_state()
                    await session.clear_audio()
                    await session.interrupt(reason="input_suspended", new_turn=False)
                    await session.state("idle")
                await runtime.broadcast("host", {"type": "input.suspend", **gateway.holders[owner]})
        if gateway.expiry:
            gateway.expiry.cancel()

        async def expire():
            while gateway.holders:
                await asyncio.sleep(
                    max(0, (min(x["expiresAt"] for x in gateway.holders.values()) - now_ms()) / 1000)
                )
                gateway.holders = {
                    key: hold for key, hold in gateway.holders.items() if hold["expiresAt"] > now_ms()
                }
            for runtime in [app.state.gateway]:
                for session in list(runtime.sessions.values()):
                    await session.sync_input_state()
                await runtime.broadcast("host", {"type": "input.resume"})

        gateway.expiry = asyncio.create_task(expire())
        return await input_status(request)

    @app.post("/api/input/resume")
    async def resume(request: Request):
        data = await body_object(request)
        gateway = app.state.gateway
        existed = gateway.holders.pop(str(data.get("owner", "")).strip()[:80], None)
        if existed and not gateway.suspended:
            if gateway.expiry:
                gateway.expiry.cancel()
            for runtime in [app.state.gateway]:
                for session in list(runtime.sessions.values()):
                    await session.sync_input_state()
                await runtime.broadcast("host", {"type": "input.resume"})
        return await input_status(request)

    @app.get("/api/tasks/{task_id}/events")
    async def task_events(task_id: str, request: Request):
        gateway = request.state.gateway
        task = gateway.tasks.tasks[task_id]

        def encode(event):
            if request.query_params.get("format") == "ag-ui":
                event = {
                    "type": "ACTIVITY_SNAPSHOT",
                    "messageId": "audio.agent.task:" + task_id,
                    "activityType": "audio.agent.task",
                    "replace": True,
                    "content": {
                        "schema": "audio.agent.task/v1",
                        "eventType": event["type"],
                        **{k: v for k, v in event.items() if k in {"task", "permission", "message"}},
                    },
                }
            return "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"

        async def events():
            queue = asyncio.Queue(maxsize=100)
            queue.task_id = task_id
            gateway.subscribers.add(queue)
            try:
                yield encode({"type": "task.snapshot", "task": public_task(task)})
                while not await request.is_disconnected():
                    try:
                        event = await asyncio.wait_for(queue.get(), 15)
                        if event.get("task", {}).get("id") == task_id:
                            yield encode(event)
                    except TimeoutError:
                        yield ": heartbeat\n\n"
            finally:
                gateway.subscribers.discard(queue)

        return StreamingResponse(events(), media_type="text/event-stream")

    @app.websocket("/api/realtime")
    async def realtime(ws: WebSocket):
        if not authorized(ws, settings):
            await ws.close(code=1008)
            return
        await ws.accept()
        gateway = app.state.gateway
        session = None
        try:
            async with asyncio.timeout(10):
                while True:
                    try:
                        first = await ws.receive_json()
                    except json.JSONDecodeError:
                        await ws.close(code=1007, reason="invalid JSON")
                        return
                    if not isinstance(first, dict):
                        await ws.close(code=1008, reason="message must be an object")
                        return
                    if (
                        isinstance(first, dict)
                        and first.get("type") == "session.hello"
                        or protocol.valid_legacy(first)
                    ):
                        break
            legacy = first.get("type") != "session.hello" if isinstance(first, dict) else False
            hello = protocol.legacy_hello(first) if legacy else first
            protocol.hello(hello)
            if hello.get("type") != "session.hello" or not hello.get("event_id"):
                raise ValueError("Expected session.hello")
            requested_protocol = hello.get("protocol", {})
            version = lambda text: tuple(int(x) for x in text.split("."))
            if (
                not version(requested_protocol.get("min", "0"))
                <= (7, 0, 0)
                <= version(requested_protocol.get("max", "0"))
            ):
                raise ValueError("Incompatible Gateway protocol; expected 7.0.0")
            instance = hello.get("client", {}).get("instance_id")
            if not isinstance(instance, str) or not 1 <= len(instance) <= 128:
                raise ValueError("client.instance_id required")
            requested = hello.get("capabilities", [])
            if (
                not isinstance(requested, list)
                or len(requested) > 64
                or not all(isinstance(x, str) for x in requested)
            ):
                raise ValueError("Invalid capabilities")
            session = await gateway.claim(ws, hello, instance, requested)
            if session is None:
                return
            if legacy and first["type"] != "connect":
                await gateway.command(session, first)
            seen = {}
            while True:
                raw = await ws.receive_text()
                if len(raw.encode()) > 20 * 1024 * 1024:
                    await ws.close(code=1009)
                    break
                try:
                    event = json.loads(raw)
                except ValueError:
                    await ws.close(code=1007, reason="invalid JSON")
                    break
                if not isinstance(event, dict):
                    await ws.close(code=1008, reason="message must be an object")
                    break
                event_id = event.get("event_id")
                if legacy:
                    if not protocol.valid_legacy(event):
                        continue
                    event_id = event.setdefault("event_id", uid())
                if not isinstance(event_id, str) or not 1 <= len(event_id) <= 128:
                    reply = {
                        "type": "error",
                        "error": {"code": "invalid_event", "message": "event_id required"},
                        "message": "event_id required",
                    }
                    await gateway.send(session, reply)
                    continue
                if event_id in seen:
                    if seen[event_id]:
                        await gateway.send(session, seen[event_id])
                    continue
                seen[event_id] = None
                if len(seen) > 2048:
                    del seen[next(iter(seen))]
                try:
                    if session.closed or gateway.sessions.get(instance) is not session:
                        break
                    if event.get("type") == "connect":
                        protocol.event(event)
                        updated = protocol.legacy_hello(event)
                        updated["client"]["instance_id"] = instance
                        updated["legacy"] = legacy
                        if not legacy:
                            updated["capabilities"] = session.hello["capabilities"]
                            updated["tools"] = event.get("clientTools", session.hello.get("tools", []))
                        session = await gateway.claim(
                            ws, updated, instance, updated["capabilities"], notify=False
                        )
                        if session is None:
                            return
                        continue
                    result = await gateway.command(session, event)
                    if result is not None:
                        response_type = (
                            "session.output_voice.updated"
                            if event["type"] == "session.output_voice.update"
                            else event["type"] + ".result"
                        )
                        reply = {"type": response_type, "request_event_id": event_id, **result}
                        seen[event_id] = reply
                        await gateway.send(session, reply)
                except (ValueError, KeyError, TypeError, ConnectionError) as exc:
                    message = str(exc)[:500] or "Invalid request"
                    reply = {
                        "type": "error",
                        "request_event_id": event_id,
                        "message": message,
                        "error": {"code": getattr(exc, "code", "invalid_request"), "message": message},
                    }
                    seen[event_id] = reply
                    await gateway.send(session, reply)
        except WebSocketDisconnect:
            pass
        except Exception as exc:
            log.warning("websocket.closed error=%s", exc, exc_info=True)
            with contextlib.suppress(Exception):
                await ws.send_json(
                    {
                        "type": "error",
                        "event_id": uid(),
                        "message": str(exc)[:500],
                        "error": {"code": "connection_error", "message": str(exc)[:500]},
                    }
                )
                await ws.close(code=1008)
        finally:
            if session:
                await session.close()
                if gateway.sessions.get(instance) is session:
                    gateway.sessions.pop(instance, None)
                if gateway.active is session:
                    gateway.active = None

    @app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def unknown_api(path: str):
        return JSONResponse({"error": "not found"}, 404)


    return app
