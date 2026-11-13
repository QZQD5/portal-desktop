import asyncio
import json

import pytest
from conftest import open_client

from gateway import protocol
from gateway.store import now_ms
from gateway.voice import VoiceSession


async def test_commands_default_to_current_conversation(running):
    gateway, _, _, port = running
    gateway.store.message("other-session", "user", "private other history")
    gateway.store.message("test-session", "user", "current history")
    for session in ("test-session", "other-session"):
        await gateway.tasks.submit(
            "future", session, schedule={"type": "reminder", "executeAt": now_ms() + 100000}
        )
    client = await open_client(port)
    try:
        await client.send("conversation.history")
        messages = (await client.wait("conversation.history.result"))["messages"]
        assert [m["content"] for m in messages] == ["current history"]
        await client.send("task.list")
        assert all(t["sessionId"] == "test-session" for t in (await client.wait("task.list.result"))["tasks"])
        await client.send("task.list", session_id="other-session")
        assert all(
            t["sessionId"] == "other-session" for t in (await client.wait("task.list.result"))["tasks"]
        )
    finally:
        await client.ws.close()


async def test_client_context_does_not_interrupt_and_respond_waits_for_playback(running):
    gateway, speech, _, port = running
    client = await open_client(port)
    client.auto_playback = False
    try:
        await client.send("input.message", text="slow original")
        await client.wait("response.started")
        generation = gateway.active.generation
        await client.send("client.event.publish", text="new ambient context")
        await client.wait("client.event.publish.result")
        assert gateway.active.generation == generation
        original = await client.wait("audio.done")
        creates = lambda: len([e for e in speech.received if e["type"] == "speech_text_buffer.commit"])
        assert creates() == 1
        await client.send("client.event.publish", text="please react", delivery_hint="respond")
        await client.wait("client.event.publish.result")
        await asyncio.sleep(0.1)
        assert creates() == 1
        await client.send("playback.ended", responseId=original["responseId"])
        reply = await client.wait(
            "transcript.final", lambda e: e["role"] == "assistant" and "please react" in e["content"]
        )
        assert reply["origin"] == "client-event"
        assert [m["content"] for m in gateway.store.history("test-session") if m["role"] == "user"] == [
            "slow original"
        ]
    finally:
        await client.ws.close()


@pytest.mark.parametrize("control", ["suspend", "mute", "sleep"])
async def test_input_controls_clear_buffers_and_cancel_old_response(running, control):
    gateway, speech, http, port = running
    client = await open_client(port)
    try:
        await client.speak(speech, "slow old")
        old = await client.wait("response.started")
        gateway.active.audio_buffer.append("AAAA")
        if control == "suspend":
            assert (await http.post("/api/input/suspend", json={"owner": "host"})).status_code == 200
        elif control == "sleep":
            await client.send("client.presence.update", state="sleeping")
        else:
            await client.send(control)
        await client.wait("playback.clear")
        await asyncio.sleep(0.1)
        assert not gateway.active.audio_buffer
        assert gateway.active.responses[old["responseId"]]["suppressed"]
        assert not any(e["type"] == "input_audio_buffer.clear" for e in speech.received)
        assert any(e["type"] == "response.cancel" for e in speech.received)
        if control == "suspend":
            await http.post("/api/input/resume", json={"owner": "host"})
        elif control == "mute":
            await client.send("unmute")
        else:
            await client.send("wake")
        await client.send("input.message", text="after control")
        assert (
            "after control"
            in (await client.wait("transcript.final", lambda e: e["role"] == "assistant"))["content"]
        )
        assert not any(
            e["type"] == "audio.delta" and e["responseId"] == old["responseId"] for e in client.events
        )
    finally:
        await client.ws.close()


@pytest.mark.parametrize(
    "values",
    [
        {"type": "task.create", "message": None},
        {"type": "task.list", "active": "false"},
        {"type": "task.list", "limit": True},
        {"type": "session.replay", "after_sequence": -1},
        {"type": "client.presence.update", "state": "unknown"},
        {"type": "client.event.publish", "text": "context", "delivery_hint": "handle"},
        {"type": "client.event.publish", "text": "context", "data": {}},
    ],
)
async def test_invalid_protocol_payload_does_not_close_socket(running, values):
    _, _, _, port = running
    client = await open_client(port)
    try:
        kind = values["type"]
        await client.send(kind, **{k: v for k, v in values.items() if k != "type"})
        await client.wait("error")
        await client.send("task.list")
        await client.wait("task.list.result")
    finally:
        await client.ws.close()


@pytest.mark.parametrize(
    "path", ["/api/input/suspend", "/api/input/resume"]
)
async def test_http_non_object_body_is_client_error(running, path):
    _, _, http, _ = running
    assert (await http.post(path, json=[])).status_code == 400
    # Express ignores non-JSON bodies; resume without an owner releases all holders.
    assert (await http.post(path, content="{invalid")).status_code == (
        200 if path == "/api/input/resume" else 400
    )


async def test_terminal_cancel_is_rejected_on_websocket(running):
    gateway, _, _, port = running
    task = await gateway.tasks.submit(
        "future", "test-session", schedule={"type": "reminder", "executeAt": now_ms() + 100000}
    )
    await gateway.tasks.cancel(task["id"])
    client = await open_client(port)
    try:
        await client.send("task.cancel", task_id=task["id"])
        assert (await client.wait("error"))["error"]["code"] == "task_not_cancellable"
    finally:
        await client.ws.close()


def test_hello_defaults_and_invalid_provider():
    hello = {
        "type": "session.hello",
        "event_id": "hello",
        "protocol": {"min": "7.0.0", "max": "7.0.0"},
        "client": {"type": "web", "instance_id": "browser"},
        "capabilities": ["input.audio"],
    }
    protocol.hello(hello)
    with pytest.raises(ValueError, match="Duplicate"):
        protocol.hello({**hello, "capabilities": ["input.audio", "input.audio"]})
    with pytest.raises(ValueError, match="Doubao Seeduplex"):
        protocol.hello({**hello, "connection": {"provider": "not-installed"}})
    for change in ({"protocol": {"min": "7", "max": "7.0.0"}}, {"connection": {"input_enabled": "false"}}):
        with pytest.raises(ValueError):
            protocol.hello({**hello, **change})


async def test_audio_defaults_follow_negotiated_capability(running):
    gateway, _, _, _ = running
    audio = VoiceSession(gateway, None, "audio", {"capabilities": ["input.audio"]})
    text = VoiceSession(gateway, None, "text", {"capabilities": ["input.text"]})
    assert audio.input_enabled and not audio.text_only
    assert not text.input_enabled and text.text_only




@pytest.mark.parametrize("audio", ["", "AQ==", "not-base64"])
async def test_invalid_pcm_is_not_forwarded(running, audio):
    _, speech, _, port = running
    client = await open_client(port)
    try:
        await client.send("audio.append", audio=audio)
        await client.wait("error")
        assert not any(e["type"] == "input_audio_buffer.append" for e in speech.received)
    finally:
        await client.ws.close()


async def test_late_auto_response_while_suspended_is_cancelled(running):
    gateway, speech, http, port = running
    client = await open_client(port)
    try:
        await http.post("/api/input/suspend", json={"owner": "host"})
        await speech.connections[-1].send(
            json.dumps({"type": "response.created", "response": {"id": "late-auto"}})
        )
        await speech.connections[-1].send(
            json.dumps({"type": "response.audio.delta", "response_id": "late-auto", "delta": "AAAA"})
        )
        await asyncio.sleep(0.05)
        assert gateway.active.responses["late-auto"]["suppressed"]
        assert any(e["type"] == "response.cancel" for e in speech.received)
    finally:
        await client.ws.close()


async def test_offline_task_result_visible_until_presented_and_history_bounded(running):
    gateway, _, http, _ = running
    task = await gateway.tasks.submit(
        "offline reminder", "history-audit", schedule={"type": "reminder", "executeAt": now_ms() - 1}
    )
    await gateway.tasks.tick_schedules()
    await gateway.tasks.publish(task, "task.notification.pending")
    history = gateway.store.history("history-audit")
    assert [m["content"] for m in history] == ["offline reminder"]
    assert history[0]["source"] == "agent-result"
    gateway.store.message(
        "history-audit",
        "assistant",
        "spoken reminder",
        id="spoken",
        source="agent-presentation",
        taskIds=[task["id"]],
    )
    assert [m["content"] for m in gateway.store.history("history-audit")] == ["spoken reminder"]
    for i in range(45):
        gateway.store.message("history-audit", "user", str(i))
    messages = (await http.get("/api/conversations/history-audit/messages")).json()["messages"]
    assert len(messages) == 40 and messages[0]["content"] == "5"


async def test_slow_sse_consumer_keeps_latest_state_and_filters_other_tasks(running):
    gateway, _, _, _ = running
    queue = asyncio.Queue(maxsize=2)
    queue.task_id = "watched"
    gateway.subscribers.add(queue)
    try:
        for state in ("running", "running", "completed"):
            await gateway.broadcast(
                "sse-audit", {"type": "task." + state, "task": {"id": "watched", "status": state}}
            )
        await gateway.broadcast(
            "sse-audit", {"type": "task.running", "task": {"id": "unrelated", "status": "running"}}
        )
        assert queue.qsize() == 2
        queue.get_nowait()
        assert queue.get_nowait()["task"]["status"] == "completed"
    finally:
        gateway.subscribers.discard(queue)
