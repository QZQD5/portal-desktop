"""Failure injection derived from upstream process-client and realtime-provider paths."""

import asyncio
import base64
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import open_client

from gateway.errors import GatewayError
from gateway.external_tools import policy
from gateway.transport import ReconnectBackoff, within_budget
from gateway.voice import Reply, VoiceSession

ORACLE = json.loads(
    (Path(__file__).parent / "fixtures/failure-upstream-oracle.json").read_text(encoding="utf-8")
)


def test_original_websocket_budget_decisions():
    for case in ORACLE["budget"]:
        assert within_budget(case["size"], case["buffered"], audio=case["audio"]) == case["accepted"]


def test_original_reconnect_backoff_and_stable_reset(monkeypatch):
    for case in ORACLE["backoff"]:
        monkeypatch.setattr("gateway.transport.random.random", lambda value=case["random"]: value)
        backoff = ReconnectBackoff()
        assert [int(backoff.next(0.1) * 1000) for _ in range(9)] == case["delays"]
        assert int(backoff.next(10) * 1000) == case["reset"]


async def test_malformed_speech_frame_is_ignored_without_reconnect(running):
    _, speech, _, port = running
    speech.prelude = ["malformed handshake frame"]
    client = await open_client(port)
    try:
        await speech.connections[-1].send("not json")
        await client.speak(speech, "after malformed frame")
        event = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert event["content"] == "Reply: after malformed frame"
        assert len(speech.connections) == 1
    finally:
        await client.ws.close()


async def test_cancel_send_race_keeps_response_pump_alive(running, monkeypatch):
    gateway, speech, _, port = running
    client = await open_client(port)
    session = gateway.active
    gateway.settings.response_inactivity_timeout = 0.02
    original_send = session.upstream_send
    original_finish = speech.finish
    monkeypatch.setattr(speech, "finish", AsyncMock())
    injected = False

    async def send(event):
        nonlocal injected
        if event["type"] == "response.cancel" and not injected:
            injected = True
            await session.upstream.close()
            raise ConnectionError("Disconnected while cancelling")
        await original_send(event)

    monkeypatch.setattr(session, "upstream_send", send)
    try:
        await session.enqueue(Reply(session.generation, response={"instructions": "stalled speech"}))
        await client.wait("response.started")
        await client.wait("error", lambda e: e.get("error", {}).get("code") == "response_failed")
        await client.wait("voice.ready")
        monkeypatch.setattr(speech, "finish", original_finish)
        await client.send("input.message", text="next after recovery race")
        reply = await client.wait("transcript.final", lambda e: e["role"] == "assistant", timeout=5)
        assert reply["content"] == "Being result: next after recovery race"
        assert injected
    finally:
        await client.ws.close()


async def test_historical_cancel_does_not_shorten_current_slot_wait(running):
    gateway, speech, _, port = running
    client = await open_client(port)
    try:
        await client.send("input.message", text="first")
        first = await client.wait("audio.done")
        connections = len(speech.connections)
        session = gateway.active
        for info in session.responses.values():
            info["suppressed"] = True
        gateway.settings.cancel_timeout = 0.05
        await client.speak(speech, "slow current")
        await client.wait("response.started", lambda e: e["responseId"] != first["responseId"])
        reply = Reply(session.generation, context_only=True)
        await session.enqueue(reply)
        await asyncio.wait_for(reply.done.wait(), 3)
        assert not reply.failed and len(speech.connections) == connections
    finally:
        await client.ws.close()


async def test_simultaneous_recovery_sends_one_cancel(running, monkeypatch):
    gateway, _, _, port = running
    client = await open_client(port)
    session = gateway.active
    calls = []

    async def cancel(event):
        calls.append(event)
        await asyncio.sleep(0.02)
        session.idle.set()

    monkeypatch.setattr(session, "upstream_send", cancel)
    try:
        session.idle.clear()
        await asyncio.gather(session.recover_response(), session.recover_response())
        assert calls == [{"type": "response.cancel"}] * ORACLE["recovery"]["cancels"]
    finally:
        await client.ws.close()


async def test_external_and_client_errors_keep_retryable_receipts(running, monkeypatch):
    gateway, _, _, port = running
    client = await open_client(port)
    try:
        session = gateway.active
        name = gateway.tools.external.add(
            "mcp", "audit", "broken", "Broken tool", {"type": "object"}, policy({"enabled": True}, 1000)
        )

        async def broken(*args):
            raise TimeoutError("external timeout")

        monkeypatch.setattr(gateway.tools.external, "execute", broken)
        assert await gateway.tools.execute(name, {}, session) == ORACLE["external"]
        session.client_tools["audit_client"] = {
            "name": "audit_client",
            "description": "test",
            "inputSchema": {"type": "object"},
        }

        async def disconnected(*args):
            raise GatewayError("client_action_disconnected", "Client disconnected")

        monkeypatch.setattr(session.actions, "request", disconnected)
        result = await gateway.tools.execute("audit_client", {}, session)
        assert result["error_code"] == "client_action_disconnected" and result["retryable"] is True
    finally:
        await client.ws.close()


async def test_slow_web_client_is_disconnected_at_budget(running):
    gateway, _, _, _ = running

    class SlowSocket:
        def __init__(self):
            self.entered, self.release = asyncio.Event(), asyncio.Event()
            self.closed = False

        async def send_text(self, event):
            self.entered.set()
            await self.release.wait()

        async def close(self, **kwargs):
            self.closed = True
            self.release.set()

    socket = SlowSocket()
    session = VoiceSession(gateway, socket, "slow-audit", {})
    first = asyncio.create_task(gateway.send(session, {"type": "audio.delta", "audio": "x" * 300000}))
    await socket.entered.wait()
    await asyncio.wait_for(first, 0.1)
    assert not socket.release.is_set()
    await gateway.send(session, {"type": "audio.delta", "audio": "x" * 300000})
    await asyncio.wait_for(first, 1)
    assert socket.closed and session.client_send_failed and session.client_pending_bytes == 0


async def test_upstream_audio_backpressure_closes_without_send(running):
    gateway, _, _, _ = running

    class SlowUpstream:
        def __init__(self):
            self.closed = False
            self.transport = SimpleNamespace(get_write_buffer_size=lambda: 512 * 1024)

        async def close(self):
            self.closed = True

        async def send(self, data):
            pytest.fail("Exceeded audio budget must not be sent")

    session = VoiceSession(gateway, None, "upstream-budget", {})
    upstream = session.upstream = SlowUpstream()
    with pytest.raises(ConnectionError, match="backpressure"):
        await session.upstream_send({"type": "input_audio_buffer.append", "audio": "AAA="})
    assert upstream.closed


async def test_disconnected_audio_preserves_utterance_prefix_within_budget(running):
    gateway, _, _, _ = running
    session = VoiceSession(gateway, None, "buffer-audit", {"capabilities": ["input.audio"]})
    chunks = [base64.b64encode(index.to_bytes(2, "little")).decode() for index in range(35)]
    for audio in chunks:
        await session.append_audio(audio)
    assert session.audio_buffer == chunks


@pytest.mark.parametrize("destination", ["client", "upstream"])
async def test_sender_cancelled_before_start_releases_budget(running, monkeypatch, destination):
    gateway, _, _, _ = running
    session = VoiceSession(gateway, None, "cancel-before-send", {})
    session.upstream = object()
    create_task = asyncio.create_task

    def cancelled_sender(coro, **kwargs):
        task = create_task(coro, **kwargs)
        if coro.__name__ in {"send_client_payload", "send_upstream_payload"}:
            task.cancel()
        return task

    monkeypatch.setattr(asyncio, "create_task", cancelled_sender)
    if destination == "client":
        await gateway.send(session, {"type": "audio.delta", "audio": "AAA="})
    else:
        await session.upstream_send({"type": "input_audio_buffer.append", "audio": "AAA="})
    await asyncio.sleep(0)
    assert session.client_pending_bytes == 0 and not session.upstream_pending_bytes
    assert not session.client_send_tasks and not session.upstream_send_tasks
