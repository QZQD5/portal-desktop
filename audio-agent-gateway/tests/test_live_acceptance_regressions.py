import asyncio
import base64
import json
import time
from unittest.mock import AsyncMock

import pytest
from conftest import open_client
from test_context_regressions import context_from
from test_context_regressions import voice as voice_fixture

from gateway.voice import Reply

voice = voice_fixture


async def test_initial_and_changed_text_updates_the_same_provider_session(running):
    gateway, provider, _, port = running
    provider.context_answers = True
    client = await open_client(port, text_only=True)
    try:
        await client.send("input.message", text="colour blue, number 47")
        await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        await client.send("unmute")
        await client.wait("voice.ready")
        await client.speak(provider, "which colour and number?")
        answer = await client.wait(
            "transcript.final", lambda e: e["role"] == "assistant" and e.get("responseId")
        )
        assert answer["content"] == "Reply: colour blue, number 47"
        done = await client.wait("audio.done")
        await client.send("playback.ended", responseId=done["responseId"])
        connections = len(provider.connections)
        await asyncio.sleep(0.45)
        assert len(provider.connections) == connections  # Native turns don't restart the provider.

        await client.send("input.message", text="number changed to 83")
        task = (
            await client.wait("task.completed", lambda e: e["task"]["objective"] == "number changed to 83")
        )["task"]
        await client.wait("transcript.final", lambda e: task["id"] in e.get("taskIds", []))
        done = await client.wait("audio.done")
        await client.send("playback.ended", responseId=done["responseId"])
        await client.speak(provider, "what is the number now?")
        answer = await client.wait(
            "transcript.final", lambda e: e["role"] == "assistant" and e.get("origin") == "model"
        )
        assert answer["content"] == "Reply: number changed to 83"
        assert len(provider.connections) == connections
        assert len(gateway.tasks.tasks) == 2
    finally:
        await client.ws.close()


async def test_queued_announcement_waits_through_native_thinking_and_playback(running):
    gateway, provider, _, port = running
    provider.voice_delay = 0.8
    client = await open_client(port)
    client.auto_playback = False
    try:
        await client.speak(provider, "remember number 83")
        await client.wait("transcript.final", lambda e: e["role"] == "user")
        session = gateway.active
        reply = Reply(session.generation, origin="announcement", speech_text="old background result")
        await session.enqueue(reply)
        await asyncio.sleep(0.5)
        assert session.native_pending_since
        assert not any(e["type"] == "speech_text_buffer.commit" for e in provider.received)
        native = await client.wait("audio.done")
        await asyncio.sleep(0.1)
        assert not any(e["type"] == "speech_text_buffer.commit" for e in provider.received)
        await client.send("playback.ended", responseId=native["responseId"])
        announced = await client.wait("transcript.final", lambda e: e.get("origin") == "announcement")
        assert announced["content"] == "old background result"
    finally:
        await client.ws.close()


async def test_voice_immediately_after_typed_input_uses_the_new_fact(running):
    gateway, provider, _, port = running
    provider.context_answers = True
    client = await open_client(port)
    try:
        await client.send("input.message", text="slow work: number changed to 83")
        await client.speak(provider, "what is the number now?")
        answer = await client.wait(
            "transcript.final", lambda e: e["role"] == "assistant" and e.get("origin") == "model"
        )
        assert answer["content"] == "Reply: slow work: number changed to 83"
        assert len(gateway.tasks.tasks) == 1
    finally:
        await client.ws.close()


@pytest.mark.parametrize("busy", ["speaking_user", "native_pending_since", "active", "playing", "local_barge_in"])
async def test_context_updates_during_current_turn_without_reconnecting(voice, busy):
    await voice.send_session("session.create")
    setattr(voice, busy, {"r"} if busy == "playing" else True)
    voice.close_provider = AsyncMock()
    voice.upstream_send.reset_mock()
    voice.store.message(voice.id, "assistant", "new task result", source="agent-result")
    await voice.sync_context()
    events = [c.args[0] for c in voice.upstream_send.await_args_list]
    assert [e["type"] for e in events] == ["session.update", "conversation.item.create"]
    assert context_from(events[0]["session"]["instructions"])["messages"][-1]["content"] == "new task result"
    assert getattr(voice, busy)
    assert voice.ready.is_set()
    voice.close_provider.assert_not_awaited()
    await voice.sync_context()
    assert voice.upstream_send.await_count == 2
    setattr(voice, busy, set() if busy == "playing" else 0)


async def test_runtime_configuration_updates_without_reconnecting(voice):
    await voice.send_session("session.create")
    voice.working_directory = "D:/new-workspace"
    voice.upstream_send.reset_mock()
    voice.close_provider = AsyncMock()
    await voice.update_session()
    updates = [c.args[0] for c in voice.upstream_send.await_args_list if c.args[0]["type"] == "session.update"]
    assert len(updates) == 1
    assert "D:/new-workspace" in updates[0]["session"]["instructions"]
    sent = voice.upstream_send.await_count
    await voice.sync_context()
    assert voice.upstream_send.await_count == sent and voice.ready.is_set()
    voice.close_provider.assert_not_awaited()


async def test_failed_asr_releases_pending_voice(voice):
    await voice.provider_input({"type": "input_audio_buffer.speech_started", "item_id": "u"})
    await voice.provider_input({"type": "input_audio_buffer.speech_stopped", "item_id": "u"})
    assert voice.native_pending_since
    await voice.provider_input({"type": "conversation.item.input_audio_transcription.failed", "item_id": "u"})
    assert not voice.native_pending_since


async def test_pausing_during_asr_keeps_that_turn_but_rejects_new_speech(voice):
    await voice.provider_input({"type": "input_audio_buffer.speech_started", "item_id": "before-mute"})
    turn = voice.turn
    voice.input_enabled = False
    await voice.provider_input({"type": "input_audio_buffer.speech_started", "item_id": "after-mute"})
    await voice.provider_input(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "after-mute",
            "transcript": "private conversation",
        }
    )
    await voice.provider_input({"type": "input_audio_buffer.speech_stopped", "item_id": "before-mute"})
    await voice.provider_input(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "before-mute",
            "transcript": "查下今天天气。",
        }
    )
    await voice.provider_event({"type": "response.created", "response": {"id": "weather"}})
    assert not voice.responses["weather"]["suppressed"]
    assert voice.responses["weather"]["turn_id"] == turn
    assert [m["content"] for m in voice.store.history(voice.id)] == ["查下今天天气。"]


async def test_muted_input_without_pending_turn_still_rejects_unsolicited_reply(voice):
    voice.input_enabled = False
    await voice.provider_event({"type": "response.created", "response": {"id": "unsolicited"}})
    assert voice.responses["unsolicited"]["suppressed"]


async def test_native_reply_timeout_reconnects_before_background_delivery(voice):
    voice.native_pending_since = time.monotonic() - voice.settings.response_timeout - 1
    voice.close_provider = AsyncMock()
    voice.queue_announcements = AsyncMock()
    job = asyncio.create_task(voice.announcements())
    try:
        async with asyncio.timeout(1):
            while not voice.close_provider.await_count:
                await asyncio.sleep(0.01)
        assert not voice.ready.is_set() and not voice.native_pending_since
        voice.queue_announcements.assert_not_awaited()
    finally:
        job.cancel()
        await asyncio.gather(job, return_exceptions=True)


async def test_closing_provider_drops_queued_sends_except_close(voice):
    voice.upstream.send = AsyncMock()
    voice.closing_upstream = voice.upstream
    await voice.send_upstream_payload(voice.upstream, json.dumps({"type": "input_audio_buffer.append"}))
    voice.upstream.send.assert_not_awaited()
    await voice.send_upstream_payload(voice.upstream, json.dumps({"type": "session.close"}), closing=True)
    voice.upstream.send.assert_awaited_once()


async def test_reconnect_buffer_has_byte_budget_and_reports_overflow_once(voice):
    voice.ready.clear()
    chunk = base64.b64encode(bytes(32000)).decode()
    for _ in range(5):
        await voice.append_audio(chunk)
    assert voice.audio_buffer_bytes == 160000
    with pytest.raises(ValueError, match="输入缓存已满"):
        await voice.append_audio(chunk)
    assert not voice.audio_buffer
    await voice.append_audio(chunk)
    assert not voice.audio_buffer
    await voice.clear_audio()
    await voice.append_audio(chunk)
    assert voice.audio_buffer == [chunk]
