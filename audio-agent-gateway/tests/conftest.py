import asyncio
import base64
import json
import os
import socket
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest_asyncio
import uvicorn
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from gateway.app import create_app
from gateway.config import Settings
from fastapi import FastAPI
from fastapi.responses import Response, StreamingResponse


def capture_event(path, event):
    with open(path, "a", encoding="utf-8") as output:
        output.write(json.dumps(event, ensure_ascii=False) + "\n")


class SeeduplexFixture:
    """Native voice replies and verbatim speech commits are separate paths."""

    def __init__(self):
        self.connections = []
        self.received = []
        self.jobs = set()
        self.ignore_cancel = False
        self.prelude = []
        self.voice_text = "voice input"
        self.voice_delay = 0
        self.context_answers = False
        self.silent_tool_reply = False
        self.voice_lead_in = False

    def spawn(self, coro):
        job = asyncio.create_task(coro)
        self.jobs.add(job)
        job.add_done_callback(self.jobs.discard)

    async def handle(self, ws):
        self.connections.append(ws)
        for raw in self.prelude:
            await ws.send(raw)
        state = {"text": "", "active": None, "waiting_tool": False}
        async for raw in ws:
            event = json.loads(raw)
            self.received.append(event)
            kind = event["type"]
            if kind in {"session.create", "session.update"}:
                state["instructions"] = event["session"]["instructions"]
                await ws.send(
                    json.dumps({"type": "session.created" if kind == "session.create" else "session.updated"})
                )
            elif kind == "conversation.item.create":
                assert "item" not in event
                for item in event["items"]:
                    if item.get("role") == "tool" and state["waiting_tool"]:
                        state["waiting_tool"] = False
                        if not self.silent_tool_reply:
                            self.spawn(self.finish(ws, state, state["active"], "tool completed"))
            elif kind == "speech_text_buffer.commit":
                state["text"] = event["text"]
                self.spawn(self.reply(ws, state))
            elif kind == "response.cancel" and state["active"]:
                if self.ignore_cancel:
                    continue
                rid = state["active"]
                state["active"] = None
                state["waiting_tool"] = False
                await ws.send(json.dumps({"type": "response.canceled", "response_id": rid}))
                await ws.send(
                    json.dumps(
                        {"type": "response.output_audio.delta", "response_id": rid, "audio": "AAAAAA=="}
                    )
                )
            elif kind == "input_audio_buffer.append":
                self.spawn(self.voice_turn(ws, state))
            elif kind == "input_audio_buffer.commit":
                await ws.send(json.dumps({"type": "input_audio_buffer.committed"}))
            elif kind == "session.close":
                await ws.send(json.dumps({"type": "session.closed"}))
                await ws.close()
            else:
                assert kind in {"response.cancel", "input_audio_mute.commit", "input_audio_unmute.commit"}, (
                    kind
                )

    async def voice_turn(self, ws, state):
        # ASRInfo is emitted after the native service has stopped the old response.
        state["active"] = None
        state["waiting_tool"] = False
        item_id = uuid4().hex
        text = self.voice_text
        await ws.send(
            json.dumps({"type": "conversation.item.input_audio_transcription.started", "item_id": item_id})
        )
        await asyncio.sleep(0.05)
        await ws.send(
            json.dumps(
                {
                    "type": "conversation.item.input_audio_transcription.completed",
                    "item_id": item_id,
                    "text": text,
                }
            )
        )
        state["text"] = text
        await asyncio.sleep(self.voice_delay)
        await self.reply(ws, state, native=True)

    async def reply(self, ws, state, *, native=False):
        try:
            if state["active"]:
                await ws.send(
                    json.dumps({"type": "error", "error": {"message": "Another response is in progress"}})
                )
                return
            rid = "resp_" + uuid4().hex
            state["active"] = rid
            text = state["text"]
            if native and self.context_answers:
                context = json.loads(
                    state["instructions"]
                    .split("<conversation_context>\n", 1)[1]
                    .split("\n", 1)[1]
                    .split("\n</conversation_context>", 1)[0]
                )
                text = next(
                    (m["content"] for m in reversed(context["messages"]) if m["role"] == "user"),
                    "missing context",
                )
            await ws.send(json.dumps({"type": "response.output_audio.started", "response_id": rid}))
            if "slow" in text:
                await asyncio.sleep(0.4)
                if state["active"] != rid:
                    return
            if native and text == "stall":
                return
            if native and ("delegate" in text or text == "task status"):
                state["waiting_tool"] = True
                if self.voice_lead_in:
                    await ws.send(json.dumps({
                        "type": "response.output_audio.delta", "response_id": rid, "audio": "AAAAAA=="
                    }))
                call = {
                    "name": "get_agent_task_status" if text == "task status" else "spawn_thinking",
                    "call_id": "call_" + uuid4().hex,
                    "arguments": "{}" if text == "task status" else json.dumps({"objective": "fixture task"}),
                }
                await ws.send(
                    json.dumps(
                        {"type": "response.function_call_arguments.done", "response_id": rid, "items": [call]}
                    )
                )
                # No response.done until the tool receipt arrives.
                return
            await self.finish(ws, state, rid, "Reply: " + text if native else text)
        except ConnectionClosed:
            pass

    async def finish(self, ws, state, rid, text):
        try:
            if state["active"] != rid:
                return
            await ws.send(
                json.dumps({"type": "response.output_text.delta", "response_id": rid, "text": text})
            )
            await ws.send(json.dumps({"type": "response.output_text.done", "response_id": rid, "text": text}))
            await ws.send(
                json.dumps(
                    {
                        "type": "response.output_audio.delta",
                        "response_id": rid,
                        "audio": base64.b64encode(bytes(960)).decode(),
                    }
                )
            )
            state["active"] = None
            await ws.send(json.dumps({"type": "response.output_audio.done", "response_id": rid}))
            await ws.send(json.dumps({"type": "response.done", "response": {"usage": {"total_tokens": 1}}}))
        except ConnectionClosed:
            pass


class Client:
    def __init__(self, ws):
        self.ws = ws
        self.events = []
        self.pending = []
        self.auto_playback = True
        self.playback_started = set()

    async def send(self, kind, **values):
        event = {"type": kind, "event_id": "event_" + uuid4().hex, **values}
        await self.ws.send(json.dumps(event))
        return event["event_id"]

    async def speak(self, provider, text):
        provider.voice_text = text
        await self.send("audio.append", audio="AAAAAA==")

    async def wait(self, kind, predicate=lambda event: True, timeout=8):
        for index, event in enumerate(self.pending):
            if event["type"] == kind and predicate(event):
                return self.pending.pop(index)
        async with asyncio.timeout(timeout):
            while True:
                event = json.loads(await self.ws.recv())
                if path := os.getenv("GATEWAY_PROTOCOL_CAPTURE"):
                    await asyncio.to_thread(capture_event, path, event)
                self.events.append(event)
                if (
                    self.auto_playback
                    and event["type"] == "audio.delta"
                    and event["responseId"] not in self.playback_started
                ):
                    self.playback_started.add(event["responseId"])
                    await self.send("playback.started", responseId=event["responseId"])
                if self.auto_playback and event["type"] == "audio.done":
                    await self.send("playback.ended", responseId=event["responseId"])
                if event["type"] == "session.ping":
                    await self.send("session.pong", request_event_id=event["event_id"])
                if event["type"] == kind and predicate(event):
                    return event
                self.pending.append(event)


@pytest_asyncio.fixture
async def being_service():
    app = FastAPI()
    active = {}

    @app.get("/being/api/status")
    async def status():
        return {"being_name": "Fixture"}

    @app.get("/being/api/stream/active")
    async def stream_status():
        return active or Response(status_code=204)

    @app.post("/being/api/stop")
    async def stop(body: dict):
        if active.get("stream_id") != body.get("stream_id"):
            return Response(status_code=409)
        active["finished"] = True
        return {"ok": True, "stream_id": active["stream_id"]}

    @app.post("/being/api/chat/stream")
    async def chat(body: dict):
        text = body["message"].split("\n\n以下是语音会话背景", 1)[0].split("\n\n<conversation_context>", 1)[0]
        sid = uuid4().hex
        active.update(stream_id=sid, finished=False, events=[])
        async def stream():
            yield "event: meta\ndata: " + json.dumps({"stream_id": sid}) + "\n\n"
            if "slow" in text or "hang" in text:
                await asyncio.sleep(.5)
            if not active["finished"]:
                item = {"seq": 1, "event": "content_block_delta", "data": {"delta": {"text": "Being result: " + text}}}
                active["events"].append(item)
                yield "event: content_block_delta\ndata: " + json.dumps(item["data"]) + "\n\n"
            active["finished"] = True
        return StreamingResponse(stream(), media_type="text/event-stream")

    sock = socket.socket(); sock.bind(("127.0.0.1", 0)); sock.listen()
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    job = asyncio.create_task(server.serve(sockets=[sock]))
    while not server.started:
        await asyncio.sleep(.01)
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}/being"
    finally:
        server.should_exit = True
        await job
        sock.close()


def mock_text_delegation(gateway):
    # Local text router removed; keyboard text goes straight to Being.
    return gateway


@pytest_asyncio.fixture
async def running(tmp_path, request, being_service):
    speech = SeeduplexFixture()
    async with serve(speech.handle, "127.0.0.1", 0) as provider:
        speech_port = provider.sockets[0].getsockname()[1]
        settings = Settings(
            data_dir=tmp_path,
            doubao_api_key="fixture-key",
            doubao_url=f"ws://127.0.0.1:{speech_port}",
            being_endpoint=being_service, being_token="fixture-heart-key", being_scene={"scene_id": "fixture-scene"},
            access_token="fixture-desktop-key-12345678901234567890",
            response_timeout=3,
            cancel_timeout=0.3,
        )
        overrides = dict(getattr(request, "param", {}))
        settings = replace(settings, **overrides)
        app = create_app(settings)
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=port,
                log_level="error",
                lifespan="on",
                timeout_graceful_shutdown=10,
            )
        )
        server_task = asyncio.create_task(server.serve(sockets=[sock]))
        while not server.started:
            await asyncio.sleep(0.01)
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", headers={"Authorization": "Bearer fixture-desktop-key-12345678901234567890"}) as http:
            gateway = app.state.gateway
            mock_text_delegation(gateway)
            yield gateway, speech, http, port
        server.should_exit = True
        await server_task
        for task in speech.jobs:
            task.cancel()
        await asyncio.gather(*speech.jobs, return_exceptions=True)


async def open_client(port, session="test-session", instance="test-client", headers=None, *,
                      client_type="web", being_name=None, **connection):
    ws = await connect(f"ws://127.0.0.1:{port}/api/realtime?sessionId={session}", additional_headers=headers if headers is not None else {"Authorization": "Bearer fixture-desktop-key-12345678901234567890"})
    client = Client(ws)
    await client.send(
        "session.hello",
        protocol={"min": "7.0.0", "max": "7.0.0"},
        client={"type": client_type, "instance_id": instance},
        **({"being_name": being_name} if being_name is not None else {}),
        capabilities=[
            "input.text",
            "input.audio",
            "tasks.commands",
            "playback.receipts",
            "permissions.respond",
            "tasks.input.respond",
            "conversation.history",
            "session.replay",
            "session.heartbeat",
            "session.takeover",
            "client.events",
            "client.presence",
            "session.output_voice",
        ],
        connection={"input_enabled": True, "output_enabled": True, **connection},
    )
    if connection.get("text_only") or connection.get("output_enabled") is False:
        await client.wait("gateway.connected")
    else:
        await client.wait("voice.ready")
    return client


def unit_settings(**overrides):
    return Settings(**{"being_endpoint": "http://127.0.0.1:1/being", "being_token": "unit-heart-key",
                       "being_scene": {"scene_id": "unit-scene"}, "access_token": "unit-desktop-key", **overrides})


def image_artifacts(blocks):
    return [{"artifactId": str(i), "parts": [{"mediaType": b["mimeType"], "raw": b["data"]}]} for i,b in enumerate(blocks)]


def authenticated_connect(uri, **kwargs):
    kwargs.setdefault("additional_headers", {"Authorization": "Bearer fixture-desktop-key-12345678901234567890"})
    return connect(uri, **kwargs)
