import json
from datetime import UTC

from conftest import open_client

from gateway.store import Store


async def test_headless_health_and_no_static_ui(running):
    _, _, http, _ = running
    response = await http.get("/")
    assert response.status_code == 404
    assert (await http.get("/api/health")).json()["realtimeProvider"] == "doubao-seeduplex"
    assert (await http.get("/api/missing")).status_code == 404
    assert (await http.get("/assets/missing.js")).status_code == 404
    assert (await http.get("/", headers={"Origin": "https://evil.example"})).status_code == 401
    assert (await http.get("/api/health", headers={"Host": "evil.example"})).status_code == 401


async def test_two_text_turns(running):
    _, _, _, port = running
    client = await open_client(port)
    try:
        for text in ["first", "second"]:
            await client.send("input.message", parts=[{"type": "text", "text": text}])
            final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
            assert final["content"] == "Being result: " + text
            audio = await client.wait("audio.done")
            await client.send("playback.ended", responseId=audio["responseId"])
        await client.send("conversation.history")
        history = await client.wait("conversation.history.result")
        assert len(history["messages"]) == 4
    finally:
        await client.ws.close()


async def test_text_barge_in_preserves_context_without_delegating_old_voice(running):
    gateway, speech, _, port = running
    client = await open_client(port)
    try:
        await client.speak(speech, "slow old reply")
        old = await client.wait("response.started")
        await client.send("input.message", parts=[{"type": "text", "text": "new reply"}])
        await client.wait(
            "transcript.final",
            lambda e: e["role"] == "assistant" and "Being result: new reply" in e["content"],
        )
        assert {task["objective"]: task["status"] for task in gateway.tasks.tasks.values()} == {
            "new reply": "completed",
        }
        assert any(row["content"] == "slow old reply" for row in gateway.history.page())
        assert any(e["type"] == "response.cancel" for e in speech.received)
        assert not any(
            e["type"] == "audio.delta" and e["responseId"] == old["responseId"] for e in client.events
        )
    finally:
        await client.ws.close()


async def test_voice_input_and_barge_in(running):
    _, speech, _, port = running
    client = await open_client(port)
    try:
        await client.speak(speech, "slow old reply")
        old = await client.wait("response.started")
        await client.speak(speech, "voice input")
        final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert final["content"] == "Reply: voice input"
        assert not any(
            e["type"] == "audio.delta" and e["responseId"] == old["responseId"] for e in client.events
        )
        assert any(e["type"] == "input_audio_buffer.append" for e in speech.received)
    finally:
        await client.ws.close()


async def test_duplicate_function_call_executes_once(running):
    gateway, speech, _, port = running
    client = await open_client(port)
    try:
        await client.speak(speech, "delegate")
        task = await client.wait("task.completed")
        assert task["task"]["result"] == "Being result: delegate"
        assert len(gateway.tasks.tasks) == 1
        audio = next((e for e in client.events if e["type"] == "audio.done"), None)
        if not audio:
            audio = await client.wait("audio.done")
        await client.send("playback.ended", responseId=audio["responseId"])
    finally:
        await client.ws.close()


async def test_task_runs_through_voice_interrupt(running):
    _, _, _, port = running
    client = await open_client(port)
    try:
        await client.send("task.create", message={"parts": [{"type": "text", "text": "slow work"}]})
        task = (await client.wait("task.create.result"))["task"]
        await client.send("input.message", parts=[{"type": "text", "text": "hello"}])
        done = await client.wait("task.completed")
        assert done["task"]["id"] == task["id"]
        assert done["task"]["status"] == "completed"
    finally:
        await client.ws.close()


async def test_cancel_backend_task(running):
    _, _, _, port = running
    client = await open_client(port)
    try:
        await client.send("task.create", message={"parts": [{"type": "text", "text": "slow work"}]})
        task = (await client.wait("task.create.result"))["task"]
        await client.wait("task.running")
        await client.send("task.cancel", task_id=task["id"])
        event = await client.wait("task.cancelled")
        assert event["task"]["id"] == task["id"]
    finally:
        await client.ws.close()






async def test_reconnect_provider_retains_text_history(running):
    _, speech, _, port = running
    client = await open_client(port)
    try:
        await client.send("input.message", parts=[{"type": "text", "text": "hello"}])
        audio = await client.wait("audio.done")
        await client.send("playback.ended", responseId=audio["responseId"])
        connections = len(speech.connections)
        await speech.connections[-1].close()
        await client.wait("voice.ready")
        await client.send("input.message", parts=[{"type": "text", "text": "again"}])
        final = await client.wait(
            "transcript.final",
            lambda e: e["role"] == "assistant" and e.get("responseId") != audio["responseId"],
        )
        assert final["content"] == "Being result: again"
        assert len(speech.connections) > connections
        latest = [e for e in speech.received if e["type"] == "session.create"][-1]
        assert "hello" in latest["session"]["instructions"]
        assert any("again" in json.dumps(e) for e in speech.received if e["type"] == "conversation.item.create")
    finally:
        await client.ws.close()


async def test_idempotent_task_submission(running):
    gateway, _, _, port = running
    client = await open_client(port)
    try:
        event = {
            "type": "task.create",
            "event_id": "same",
            "message": {"parts": [{"type": "text", "text": "work"}]},
        }
        await client.ws.send(json.dumps(event))
        await client.ws.send(json.dumps(event))
        first = await client.wait("task.create.result")
        second = await client.wait("task.create.result")
        assert first["task"]["id"] == second["task"]["id"]
        assert len(gateway.tasks.tasks) == 1
    finally:
        await client.ws.close()


async def test_suspend_expires(running):
    _, _, http, port = running
    client = await open_client(port)
    try:
        assert (await http.post("/api/input/suspend", json={"owner": "test", "ttlMs": 100})).json()[
            "suspended"
        ]
        await client.wait("input.resume")
        assert not (await http.get("/api/input")).json()["suspended"]
    finally:
        await client.ws.close()


async def test_tools_and_reminder_persistence(running):
    gateway, _, _, port = running
    client = await open_client(port)
    session = gateway.active
    try:
        await gateway.tools.execute(
            "notes", {"action": "add", "list": "shopping", "items": ["milk"]}, session
        )
        assert gateway.tools.notes({"action": "show", "list": "shopping"})["items"][0]["text"] == "milk"
        from datetime import datetime, timedelta

        result = await gateway.tools.execute(
            "schedule_reminder",
            {
                "execute_at": (datetime.now(UTC) + timedelta(milliseconds=100)).isoformat(),
                "reminder": "drink water",
            },
            session,
        )
        event = await client.wait("task.completed")
        assert event["task"]["id"] == result["task_id"]
    finally:
        await client.ws.close()


def test_sqlite_history_and_replay(tmp_path):
    path = tmp_path / "store.sqlite3"
    store = Store(path)
    store.message("one", "user", "hello")
    event = store.event("one", {"type": "transcript.final", "role": "user", "content": "hello"})
    store.close()
    store = Store(path)
    assert store.history("one")[0]["content"] == "hello"
    assert store.replay("one")["events"][0]["sequence"] == event["sequence"]
    assert store.history("two") == []
    store.close()
