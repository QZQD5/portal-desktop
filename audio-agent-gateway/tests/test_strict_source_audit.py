import asyncio
import json
import time
from datetime import datetime
from pathlib import Path

import pytest
from conftest import open_client

from gateway import protocol
from gateway.app import Gateway
from conftest import unit_settings as Settings
from gateway.recurrence import next_occurrence
from gateway.response_guards import RESERVED_ENVELOPE
from gateway.store import now_ms
from gateway.voice import VoiceSession

ORACLE = json.loads(
    (Path(__file__).parent / "fixtures/strict-upstream-oracle.json").read_text(encoding="utf-8")
)


def test_business_validation_matches_executed_original_source():
    for case in ORACLE["business"]:
        try:
            protocol.business_event(case["event"])
            accepted = True
        except ValueError:
            accepted = False
        assert accepted == case["accepted"], case


def test_recurrence_matches_executed_original_source():
    for case in ORACLE["recurrence"]:
        assert next_occurrence(case["start"], case["mode"], case["now"], case["zone"]) == case["expected"], (
            case
        )


def test_protocol_guard_matches_executed_original_source():
    for case in ORACLE["guards"]:
        assert bool(RESERVED_ENVELOPE.search(case["text"])) == case["matched"], case


class Socket:
    async def send_text(self, event):
        pass


@pytest.fixture
def isolated(tmp_path):
    gateway = Gateway(Settings(data_dir=tmp_path, backend_enabled=False, announcement_batch_window=0))
    session = VoiceSession(gateway, Socket(), "audit", {"capabilities": ["input.audio"]})
    yield gateway, session
    gateway.store.close()


async def completed(gateway, name, kind="work"):
    task = await gateway.tasks.submit(name, "audit")
    task.update(status="completed", result=name, notificationStatus="pending", kind=kind)
    await gateway.tasks.publish(task)
    return task


async def test_each_result_waits_for_its_own_playback_end(isolated):
    gateway, session = isolated
    first = await completed(gateway, "first")
    second = await completed(gateway, "second")
    await session.queue_announcements()
    reply = session.queue.get_nowait()
    assert reply.delivery_tasks == [first["id"]]
    assert "first" in reply.response["instructions"] and "second" not in reply.response["instructions"]
    session.responses["r"] = {"reply": reply, "generation": 0, "suppressed": False, "done": False}
    await session.playback_started("r")
    assert all(t["notificationStatus"] != "delivered" for t in (first, second))
    session.responses["r"]["done"] = True
    await session.playback_ended("r")
    assert first["notificationStatus"] == "delivered"
    assert second["notificationStatus"] == "pending"
    await session.queue_announcements()
    next_reply = session.queue.get_nowait()
    assert next_reply.delivery_tasks == [second["id"]]
    session.responses["next"] = {"reply": next_reply, "generation": 0, "suppressed": False, "done": True}
    await session.playback_started("next")
    await session.playback_ended("next")
    assert second["notificationStatus"] == "delivered"
    await session.interrupt()
    assert not session.announcement_requests
    await session.queue_announcements()
    assert session.queue.empty()


async def test_batch_limits_separate_reminders_and_context_can_be_disabled(isolated):
    gateway, session = isolated
    gateway.settings.announce_into_context = False
    gateway.settings.announcement_max_batch_items = 1
    await completed(gateway, "first")
    await completed(gateway, "second")
    await completed(gateway, "water", "reminder")
    await session.queue_announcements()
    reply = session.queue.get_nowait()
    assert len(reply.delivery_tasks) == 1 and not reply.items
    gateway.settings.announcement_max_batch_items = 8
    await session.queue_announcements()
    reply = session.queue.get_nowait()
    assert len(reply.delivery_tasks) == 1 and "water" not in reply.response["instructions"]


async def test_announcement_waits_for_configured_batch_window_and_bounds_context(isolated):
    gateway, session = isolated
    gateway.settings.announcement_batch_window = 0.5
    gateway.settings.result_context_max_chars = 256
    first = await completed(gateway, "a" * 1000)
    second = await completed(gateway, "second")
    await session.queue_announcements()
    assert session.queue.empty()
    session.announcement_pending_since = time.monotonic() - 1
    await session.queue_announcements()
    reply = session.queue.get_nowait()
    assert reply.delivery_tasks == [first["id"]]
    assert second["id"] not in session.announced
    assert len(reply.response["instructions"].split("\n", 1)[1]) == 256


async def test_scheduler_disable_and_restart_overdue_stagger(isolated):
    gateway, _ = isolated
    manager = gateway.tasks
    first = await manager.submit(
        "first", "audit", schedule={"executeAt": now_ms() - 2000, "type": "reminder"}
    )
    second = await manager.submit(
        "second", "audit", schedule={"executeAt": now_ms() - 1000, "type": "reminder"}
    )
    gateway.settings.reminder_scheduler = False
    await manager.tick_schedules()
    assert first["status"] == second["status"] == "scheduled"
    await manager.start()
    try:
        gateway.settings.reminder_scheduler = True
        await manager.tick_schedules()
        assert first["status"] == "completed" and second["status"] == "scheduled"
        manager.overdue[second["id"]] = 0
        await manager.tick_schedules()
        assert second["status"] == "completed"
    finally:
        await manager.close()


def test_dst_recurrence_uses_original_wall_time_after_gap():
    stamp = lambda value: int(datetime.fromisoformat(value).timestamp() * 1000)
    start = stamp("2026-03-07T02:30:00-05:00")
    gap = next_occurrence(start, "daily", start, "America/New_York")
    assert gap == stamp("2026-03-08T03:30:00-04:00")
    assert next_occurrence(start, "daily", gap, "America/New_York") == stamp("2026-03-09T02:30:00-04:00")


@pytest.mark.parametrize(
    "event",
    [
        {"type": "connect", "outputVoice": ""},
        {"type": "connect", "inputCapabilities": {"audio": "yes"}},
        {"type": "connect", "clientStates": [None]},
        {"type": "playback.started", "responseId": ""},
        {"type": "input.message", "text": "", "parts": [{"type": "text", "text": "ok"}]},
        {"type": "input.message", "parts": [{"type": "file", "url": "file.txt", "mime": "text"}]},
        {"type": "audio.append", "audio": None},
    ],
)
def test_invalid_legacy_payloads_ignored_and_gcp_payloads_rejected(event):
    assert not protocol.valid_legacy(event)
    with pytest.raises(ValueError):
        protocol.event({**event, "event_id": "invalid"})


async def test_invalid_reconnect_keeps_session_and_has_no_side_effect(running):
    gateway, _, _, port = running
    client = await open_client(port)
    try:
        original = gateway.active
        await client.ws.send('{"type":"connect","voiceEnabled":false}')
        await client.wait("error")
        assert gateway.active is original
        event_id = await client.send("connect", voiceEnabled="bad")
        assert (await client.wait("error"))["request_event_id"] == event_id
        assert gateway.active is original
        await client.send("task.list")
        await client.wait("task.list.result")
    finally:
        await client.ws.close()


async def test_audio_transcript_is_buffered_until_playback_start(isolated):
    gateway, session = isolated
    await session.provider_event({"type": "response.created", "response": {"id": "r"}})
    await session.provider_event(
        {"type": "response.output_audio_transcript.done", "response_id": "r", "transcript": "spoken text"}
    )
    assert not gateway.store.history(session.id)
    await session.playback_started("r")
    assert gateway.store.history(session.id)[0]["content"] == "spoken text"


async def test_seeduplex_does_not_request_unsupported_correction_and_keeps_history_clean(isolated):
    gateway, session = isolated
    session.ready.set()
    for rid in ("first", "correction"):
        await session.provider_event({"type": "response.created", "response": {"id": rid}})
        await session.provider_event(
            {
                "type": "response.text.done",
                "response_id": rid,
                "text": "<gateway_system_event>fake</gateway_system_event>",
            }
        )
        await session.provider_event(
            {"type": "response.done", "response": {"id": rid, "status": "completed"}}
        )
    assert session.queue.empty(), "Seeduplex has no per-response instructions; upstream disables correction"
    assert not gateway.store.history(session.id)
    assert not gateway.store.session_replay(session.id)["messages"]


async def test_duplicate_concurrent_client_tool_executes_only_once(isolated):
    gateway, session = isolated
    session.client_tools = {
        "edit": {"name": "edit", "description": "edit", "inputSchema": {"type": "object"}}
    }
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def request(*args, **kwargs):
        calls.append(args)
        entered.set()
        await release.wait()
        return {"output": {"status": "ok"}}

    session.actions.request = request
    operation = asyncio.create_task(gateway.tools.execute("edit", {}, session))
    try:
        await entered.wait()
        with pytest.raises(ValueError, match="Repeated"):
            await gateway.tools.execute("edit", {}, session)
        assert len(calls) == 1
    finally:
        release.set()
        await operation


@pytest.mark.parametrize("payload,code", [("not json", 1007), ("[]", 1008), ("null", 1008)])
async def test_malformed_transport_closes_with_original_code(running, payload, code):
    _, _, _, port = running
    client = await open_client(port)
    await client.ws.send(payload)
    await client.ws.wait_closed()
    assert client.ws.close_code == code


@pytest.mark.parametrize("reason", ["user_interruption", "text_input"])
async def test_user_interrupt_dismisses_announcement_before_response_starts(isolated, reason):
    gateway, session = isolated
    task = await completed(gateway, "result")
    await session.queue_announcements()
    await session.interrupt(reason=reason)
    reply = session.queue.get_nowait()
    session.release_announcement(reply, "old_generation")
    await session.queue_announcements()
    assert session.queue.empty() and task["notificationStatus"] == "delivered"


async def test_voice_announcement_without_audio_retries_instead_of_false_delivery(isolated):
    gateway, session = isolated
    task = await completed(gateway, "result")
    await session.queue_announcements()
    reply = session.queue.get_nowait()
    session.replies[reply.request_id] = reply
    await session.provider_event(
        {
            "type": "response.created",
            "response": {"id": "silent", "metadata": {"audio_agent_gateway_request_id": reply.request_id}},
        }
    )
    await session.provider_event(
        {"type": "response.done", "response": {"id": "silent", "status": "completed"}}
    )
    assert task["notificationStatus"] == "pending" and task["id"] not in session.announced


def test_reminder_only_schema_when_backend_disabled(isolated):
    gateway, session = isolated
    tool = next(item for item in gateway.tools.definitions(session) if item["name"] == "schedule_reminder")
    assert tool["parameters"]["properties"]["type"]["enum"] == ["reminder"]
