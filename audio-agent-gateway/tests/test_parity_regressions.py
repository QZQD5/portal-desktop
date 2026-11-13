import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from conftest import open_client

from gateway.app import Gateway
from conftest import unit_settings as Settings
from gateway.errors import GatewayError
from gateway.store import Store, now_ms
from gateway.voice import Reply, VoiceSession
from test_voice_requests import call, native, speech  # noqa: F401


class Socket:
    async def send_text(self, event):
        pass


@pytest.fixture
def isolated(tmp_path):
    gateway = Gateway(Settings(data_dir=tmp_path, backend_enabled=False))
    session = VoiceSession(gateway, Socket(), "session-a", {})
    yield gateway, session
    gateway.store.close()


async def active_task(gateway, session):
    task = await gateway.tasks.submit("work", session.id)
    task["status"] = "running"
    gateway.tasks.active = task
    return task


async def test_same_turn_task_submission_is_idempotent_and_loop_bounded(isolated):
    gateway, session = isolated
    args = {"objective": "do the same work"}
    one, two = await asyncio.gather(
        gateway.tools.execute("spawn_thinking", args, session),
        gateway.tools.execute("spawn_thinking", args, session),
    )
    assert one["task_id"] == two["task_id"] and len(gateway.tasks.tasks) == 1
    for index in range(11):
        await gateway.tools.execute("notes", {"action": "show", "list": str(index)}, session)
    with pytest.raises(ValueError, match="limit"):
        await gateway.tools.execute("get_current_time", {}, session)


@pytest.mark.parametrize("case", ["limit", "repeat", "spawn_receipt"])
async def test_tool_loop_guard_returns_native_receipt_without_extra_response(isolated, case):
    gateway, session = isolated
    if case == "spawn_receipt":
        name, args = "spawn_thinking", {"objective": "one task only"}
    else:
        name, args = "notes", {"action": "lists"}
    await gateway.tools.execute(name, args, session)
    if case == "limit":
        session.tool_turns[session.turn]["count"] = 12
    session.responses["loop"] = {
        "generation": session.generation,
        "provider_epoch": session.provider_epoch,
        "suppressed": False,
        "done": True,
        "calls": {"loop-call": None},
        "followup": False,
    }
    if case == "spawn_receipt":
        session.responses["loop"]["request"] = {
            "turn": session.turn, "text": args["objective"], "confirmed": True, "context": {},
        }
    session.ready.set()
    session.send_item = AsyncMock()
    await session.run_tool("loop", "loop-call", {"name": name, "arguments": json.dumps(args)})
    assert session.responses["loop"]["terminal_tool"]
    assert session.queue.empty(), "Native continuation must not request another response"
    receipts = session.send_item.call_args_list
    assert len(receipts) == 1
    result = json.loads(receipts[0].args[0]["output"])
    assert result.get("error", False) == (case != "spawn_receipt")
    await session.tool_followup("loop")
    assert session.send_item.call_count == 1
    assert len(gateway.tasks.tasks) == (1 if case == "spawn_receipt" else 0)


async def test_tool_free_summary_cannot_execute_tools_or_restart_loop(isolated, monkeypatch):
    gateway, session = isolated
    events = []

    async def emit(event):
        events.append(event)

    async def forbidden(*args):
        pytest.fail("A tool-free summary must not execute a tool")

    monkeypatch.setattr(session, "emit", emit)
    monkeypatch.setattr(gateway.tools, "execute", forbidden)
    session.responses["summary"] = {
        "generation": session.generation,
        "provider_epoch": session.provider_epoch,
        "suppressed": False,
        "done": True,
        "calls": {"call": None},
        "followup": False,
        "reply": Reply(session.generation, response={"tool_choice": "none"}),
    }
    session.ready.set()
    session.send_item = AsyncMock()
    await session.run_tool("summary", "call", {"name": "notes", "arguments": '{"action":"lists"}'})
    assert session.queue.empty()
    assert json.loads(session.send_item.call_args.args[0]["output"])["code"] == "tool_loop_limit"


@pytest.mark.parametrize("change", ["interrupt", "reconnect"])
async def test_tool_guard_does_not_answer_an_obsolete_turn(isolated, change):
    _, session = isolated
    session.responses["old"] = {
        "generation": session.generation,
        "provider_epoch": session.provider_epoch,
        "suppressed": False,
        "done": True,
        "calls": {"call": {"error": True, "code": "tool_loop_limit"}},
        "followup": False,
        "terminal_tool": True,
        "tool_guard": True,
    }
    if change == "interrupt":
        session.generation += 1
    else:
        session.provider_epoch += 1
    await session.tool_followup("old")
    assert session.queue.empty()


def test_voice_delegates_web_research_to_agent_by_default(isolated):
    gateway, session = isolated
    definitions = {item["name"]: item for item in gateway.tools.definitions(session)}
    assert "web_search" not in definitions and "fetch_url" not in definitions
    assert "新闻查询" in definitions["spawn_thinking"]["description"]
    assert "统一调用 `spawn_thinking`" in session.instructions()


async def test_interrupt_between_tool_scheduling_and_execution_has_no_side_effect(isolated):
    gateway, session = isolated
    session.responses["old"] = {
        "generation": 0,
        "provider_epoch": 0,
        "suppressed": False,
        "done": False,
        "calls": {},
        "followup": False,
    }
    session.start_tool(
        "old", {"call_id": "old-call", "name": "spawn_thinking", "arguments": '{"objective":"must not run"}'}
    )
    await session.interrupt()
    await asyncio.gather(*session.tool_tasks)
    assert not gateway.tasks.tasks


async def test_tool_result_limit_counts_utf8_bytes(isolated, monkeypatch):
    gateway, session = isolated

    async def oversized(*args):
        return {"content": "字" * 22000}

    monkeypatch.setattr(gateway.tools, "execute", oversized)
    session.responses["large"] = {
        "generation": session.generation,
        "provider_epoch": session.provider_epoch,
        "suppressed": False,
        "done": True,
        "calls": {"large-call": None},
        "followup": False,
    }
    session.ready.set()
    session.send_item = AsyncMock()
    await session.run_tool("large", "large-call", {"name": "fetch_url", "arguments": "{}"})
    result = session.responses["large"]["calls"]["large-call"]
    assert result["code"] == "tool_result_too_large"
    assert len(json.dumps(result).encode()) < 1000
    assert "tool_result_too_large" in session.send_item.call_args.args[0]["output"]
    assert session.queue.empty()


async def test_input_announcement_is_not_masked_by_resolved_authorization(isolated):
    gateway, session = isolated
    session.text_only = False
    task = await active_task(gateway, session)
    task["authorization"] = {"id": "resolved", "status": "approved"}
    task["inputRequest"] = {"id": "question", "status": "pending", "prompt": "Color?"}
    session.ready.set()
    runner = asyncio.create_task(session.announcements())
    try:
        reply = await asyncio.wait_for(session.queue.get(), 1)
        assert reply.announcement_key == "question" and "Color?" in reply.response["instructions"]
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


async def test_cancel_all_has_no_hundred_task_limit(native):
    gateway, session = native.gateway, native
    for i in range(105):
        await gateway.tasks.submit(str(i), session.id)
    await speech(session, "cancel", "取消全部任务")
    await call(session, "cancel-reply", "cancel_agent_task", {"all": True})
    await asyncio.gather(*session.tool_tasks)
    await asyncio.gather(*gateway.tasks.cancellations.values())
    assert all(task["status"] == "cancelled" for task in gateway.tasks.tasks.values())


async def test_disabled_library_and_terminal_cancel_http_statuses(running):
    gateway, _, http, _ = running
    assert (await http.get("/api/domain")).status_code == 404
    assert (await http.post("/api/permissions/stale", json={"decision": "task"})).status_code == 404
    task = await gateway.tasks.submit(
        "later", "test", schedule={"type": "reminder", "recurrence": "once", "executeAt": now_ms() + 60000}
    )
    await gateway.tasks.cancel(task["id"])
    assert (await http.delete(f"/api/tasks/{task['id']}")).status_code == 409


async def test_agui_sse_contract(running):
    gateway, _, http, _ = running
    task = await gateway.tasks.submit(
        "later", "test", schedule={"type": "reminder", "recurrence": "once", "executeAt": now_ms() + 60000}
    )
    async with http.stream("GET", f"/api/tasks/{task['id']}/events?format=ag-ui") as response:
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                event = json.loads(line[6:])
                assert event["type"] == "ACTIVITY_SNAPSHOT" and event["replace"]
                assert event["content"]["schema"] == "audio.agent.task/v1"
                assert event["content"]["task"]["id"] == task["id"]
                break


def test_replay_cursor_errors_session_sequences_and_durable_journal(tmp_path):
    store = Store(tmp_path / "store.db")
    try:
        assert store.event("one", {"type": "task.snapshot", "tasks": []})["sequence"] == 1
        assert store.event("two", {"type": "task.snapshot", "tasks": []})["sequence"] == 1
        with pytest.raises(GatewayError) as error:
            store.replay("one", 2)
        assert error.value.code == "session_expired"
        store.event("one", {"type": "transcript.final", "role": "user", "content": "kept", "turnId": "turn"})
        with store.db:
            store.db.execute("DELETE FROM replay_events WHERE session='one' AND sequence=1")
        with pytest.raises(GatewayError) as error:
            store.replay("one", 0)
        assert error.value.code == "sequence_expired"
        store.delivery("one", "delivered", taskIds=["t"])
        replay = store.session_replay("one")
        assert replay["messages"][0]["content"] == "kept" and replay["deliveries"]
    finally:
        store.close()


async def test_takeover_increments_lease_and_replay_error_preserves_connection(running):
    _, _, _, port = running
    first = await open_client(port)
    second = await open_client(port, instance="second", takeover=True)
    try:
        a = next(e for e in first.events if e["type"] == "session.ready")
        b = next(e for e in second.events if e["type"] == "session.ready")
        assert b["connection"]["lease_generation"] > a["connection"]["lease_generation"]
        await second.send("session.replay", after_sequence=999999)
        assert (await second.wait("error"))["error"]["code"] == "session_expired"
        await second.send("input.message", parts=[1])
        await second.wait("error")
        await second.send("input.message", text="still works")
        await second.wait("audio.done")
        await second.ws.send("invalid json")
        await second.ws.wait_closed()
        assert second.ws.close_code == 1007
    finally:
        await first.ws.close()
        await second.ws.close()


@pytest.mark.parametrize("reason", ["user_interruption", "text_input"])
async def test_user_interruption_dismisses_announcement_and_late_receipt_is_ignored(isolated, reason):
    gateway, session = isolated
    task = await gateway.tasks.submit("done", session.id)
    task.update(status="completed", notificationStatus="pending")
    reply = Reply(0, task_id=task["id"], announcement_key=task["id"])
    session.announced.add(task["id"])
    session.announcement_requests[task["id"]] = reply.request_id
    session.responses["old"] = {"reply": reply, "generation": 0, "suppressed": False, "done": True}
    await session.interrupt(reason=reason)
    await session.playback_ended("old")
    assert task["notificationStatus"] == "delivered" and task["id"] in session.announced
    assert gateway.store.session_replay(session.id)["deliveries"]


async def test_early_playback_receipt_waits_for_generation_completion(isolated):
    gateway, session = isolated
    task = await gateway.tasks.submit("done", session.id)
    task.update(status="completed", notificationStatus="pending")
    reply = Reply(0, task_id=task["id"], announcement_key=task["id"])
    info = {"reply": reply, "generation": 0, "suppressed": False, "done": False}
    session.responses["r"] = info
    await session.playback_ended("r")
    assert task["notificationStatus"] == "pending"
    info["done"] = True
    await session.playback_ended("r")
    assert task["notificationStatus"] == "delivered"


async def test_provider_reconnect_redelivers_unacknowledged_task(running):
    gateway, speech, _, port = running
    client = await open_client(port)
    client.auto_playback = False
    try:
        await client.send("task.create", message={"parts": [{"type": "text", "text": "result delivery"}]})
        task = (await client.wait("task.completed"))["task"]
        old = (await client.wait("audio.done"))["responseId"]
        await speech.connections[-1].close()
        await client.wait("voice.ready")
        new = (await client.wait("audio.done"))["responseId"]
        assert new != old
        await client.send("playback.ended", responseId=old)
        await asyncio.sleep(0.05)
        assert gateway.tasks.tasks[task["id"]]["notificationStatus"] == "pending"
        await client.send("playback.ended", responseId=new)
        await client.wait("task.notification.delivered")
        deliveries = gateway.store.session_replay("test-session")["deliveries"]
        assert any(e["payload"]["domainType"] == "delivery.interrupted" for e in deliveries)
        assert deliveries[-1]["payload"]["domainType"] == "delivery.delivered"
    finally:
        await client.ws.close()


async def test_missing_playback_receipt_retries_but_does_not_overlap_playing_audio(running):
    gateway, _, _, port = running
    gateway.settings.announcement_ack_timeout = 0.05
    client = await open_client(port)
    client.auto_playback = False
    try:
        await client.send("task.create", message={"parts": [{"type": "text", "text": "delivery timeout"}]})
        await client.wait("task.completed")
        old = (await client.wait("audio.done"))["responseId"]
        new = (await client.wait("audio.done"))["responseId"]
        assert new != old
        await client.send("playback.started", responseId=new)
        await asyncio.sleep(0.4)
        assert gateway.active.responses[new].get("playback_started")
        assert not gateway.active.responses[new]["suppressed"]
        await client.send("playback.ended", responseId=new)
        await client.wait("task.notification.delivered")
    finally:
        await client.ws.close()


def test_normal_being_tasks_have_no_artificial_deadline():
    settings = Settings()
    assert settings.task_timeout == 0
    assert settings.scheduled_task_timeout == 1800


def test_notes_preserve_upstream_matching_and_result_contract(isolated):
    gateway, _ = isolated
    notes = gateway.tools.notes
    result = notes(
        {"action": "add", "list": "Office Shopping", "items": ["Milk", "milk", "Oat milk", "Eggs"]}
    )
    assert result["added"] == ["Milk", "Oat milk", "Eggs"] and result["duplicates"] == ["milk"]
    shown = notes({"action": "show", "list": "office"})
    assert shown["list"] == "Office Shopping" and shown["items"][0]["id"].startswith("item_")
    notes({"action": "add", "list": "Home Shopping", "items": ["Tea"]})
    assert notes({"action": "clear", "list": "Shopping"})["status"] == "ambiguous"
    assert notes({"action": "remove", "list": "office", "items": ["mil"]})["status"] == "ambiguous"
    assert notes({"action": "remove", "list": "office", "items": ["eg"]})["removed"] == ["Eggs"]
    assert notes({"action": "show", "list": "absent"})["status"] == "not_found"
    assert notes({"action": "clear", "list": "office"})["removed"] == 2
    assert notes({"action": "lists"})["lists"][0].keys() == {"list", "count", "updated_at"}


async def test_citations_use_consistent_ids_across_multiple_tools(isolated, monkeypatch):
    gateway, session = isolated
    session.citations = [{"id": "source_1", "url": "https://one.example/", "title": "one"}]

    async def fetch(*args):
        return {
            "citations": [{"id": "1", "url": "https://two.example", "title": "two"}],
            "results": [{"id": "1", "url": "https://two.example"}],
        }

    monkeypatch.setattr(gateway.tools, "execute", fetch)
    session.responses["r"] = {
        "generation": 0,
        "provider_epoch": 0,
        "suppressed": False,
        "done": False,
        "calls": {},
        "followup": False,
    }
    await session.run_tool("r", "call", {"name": "web_search", "arguments": '{"query":"example"}'})
    result = session.responses["r"]["calls"]["call"]
    assert result["citations"][0]["id"] == result["results"][0]["citation_id"] == "source_2"
