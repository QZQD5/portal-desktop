import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.app import Gateway
from conftest import mock_text_delegation
from conftest import unit_settings as Settings
from gateway.voice import Reply, VoiceSession


@pytest.fixture
async def voice(tmp_path):
    gateway = Gateway(
        Settings(data_dir=tmp_path, backend_enabled=False, cancel_timeout=0.04, response_timeout=5)
    )
    mock_text_delegation(gateway)
    session = VoiceSession(gateway, None, "duplex", {"capabilities": ["input.audio"]})
    session.events, session.sent = [], []

    async def emit(event):
        session.events.append(event)
        await asyncio.sleep(0)

    async def send(event):
        session.sent.append(event)
        await asyncio.sleep(0)

    session.emit, session.upstream_send = emit, send
    session.upstream = SimpleNamespace(close=AsyncMock())
    session.ready.set()
    yield session
    await session.close()
    await gateway.close()


async def created(session, rid, metadata=None):
    await session.provider_event(
        {"type": "response.created", "response": {"id": rid, "metadata": metadata or {}}}
    )


async def test_voice_interrupt_before_response_created_recovers_without_next_text(voice):
    voice.idle.clear()
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "voice-new"})
    async with asyncio.timeout(0.5):
        while not voice.upstream.close.called:
            await asyncio.sleep(0.01)
    assert any(e["type"] == "playback.clear" for e in voice.events)
    assert any(e["type"] == "response.cancel" for e in voice.sent)


async def test_late_automatic_response_during_cancel_cannot_restart_audio(voice):
    await created(voice, "old")
    await voice.interrupt()
    await created(voice, "late-auto")
    await voice.provider_event({"type": "response.audio.delta", "response_id": "late-auto", "delta": "AAA="})
    assert not any(e["type"] == "audio.delta" for e in voice.events)


async def test_unrelated_no_active_error_does_not_release_live_response(voice):
    await created(voice, "current")
    await voice.provider_event({"type": "error", "error": {"message": "No active response"}})
    assert voice.active == "current" and not voice.idle.is_set()


async def test_interrupt_between_item_and_create_does_not_send_old_response(voice, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    original = voice.update_session

    async def update():
        entered.set()
        await release.wait()
        await original()

    monkeypatch.setattr(voice, "update_session", update)
    reply = Reply(voice.generation, items=[])
    await voice.enqueue(reply)
    pump = asyncio.create_task(voice.response_pump())
    try:
        await entered.wait()
        await voice.interrupt()
        release.set()
        await asyncio.wait_for(reply.done.wait(), 0.2)
        assert not any(e["type"] == "response.create" for e in voice.sent)
    finally:
        release.set()
        pump.cancel()
        await asyncio.gather(pump, return_exceptions=True)


async def test_text_supersedes_pending_voice_transcription(voice):
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "old-input"})
    old_turn = voice.turn
    await voice.input([{"type": "text", "text": "new text"}])
    await voice.provider_event(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "old-input",
            "transcript": "stale speech",
        }
    )
    late = next(m for m in voice.store.history() if m["content"] == "stale speech")
    assert late["source"] == "voice-user-delayed" and late["turnId"] == old_turn
    assert [task["objective"] for task in voice.gateway.tasks.tasks.values()] == ["new text"]


async def test_manual_input_guard_releases_on_agent_submission(voice):
    await voice.input([{"type": "text", "text": "manual"}])
    generation = voice.generation
    assert voice.manual_generation is None
    assert voice.queue.empty()
    assert next(iter(voice.gateway.tasks.tasks.values()))["objective"] == "manual"
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "new-speech"})
    assert voice.generation == generation + 1


async def test_late_voice_reply_after_typed_submission_is_suppressed(voice):
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "old-input"})
    await voice.input([{"type": "text", "text": "manual"}])
    assert voice.manual_generation is None
    await created(voice, "late-old-voice")
    await voice.provider_event(
        {"type": "response.audio.delta", "response_id": "late-old-voice", "delta": "AAA="}
    )
    assert voice.responses["late-old-voice"]["suppressed"]
    assert not any(e["type"] == "audio.delta" for e in voice.events)
    await voice.provider_event(
        {"type": "input_audio_buffer.speech_started", "item_id": "new-voice", "native_asr": True}
    )
    await voice.provider_event(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "new-voice",
            "transcript": "fresh voice",
        }
    )
    await created(voice, "new-voice-response")
    assert not voice.responses["new-voice-response"]["suppressed"]


async def test_voice_final_is_deduplicated_and_invalid_input_discarded(voice):
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "one"})
    final = {
        "type": "conversation.item.input_audio_transcription.completed",
        "item_id": "one",
        "transcript": "hello",
    }
    await voice.provider_event(final)
    await voice.provider_event(final)
    assert len([e for e in voice.events if e["type"] == "transcript.final"]) == 1
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "bad"})
    await voice.provider_event(
        {"type": "input_audio_buffer.speech_stopped", "item_id": "bad", "reason": "turn_invalid"}
    )
    await voice.provider_event({**final, "item_id": "bad", "transcript": "invalid"})
    assert voice.events[-1]["type"] == "voice.state" and voice.events[-1]["state"] == "idle"
    assert not any(e.get("content") == "invalid" for e in voice.events)


async def test_audio_in_flight_at_interrupt_cannot_cross_playback_clear(voice, monkeypatch):
    await created(voice, "old")
    entered, release = asyncio.Event(), asyncio.Event()
    original = voice.state

    async def state(value):
        if value == "speaking":
            entered.set()
            await release.wait()
        await original(value)

    monkeypatch.setattr(voice, "state", state)
    audio = asyncio.create_task(
        voice.provider_event({"type": "response.audio.delta", "response_id": "old", "delta": "AAA="})
    )
    await entered.wait()
    await voice.interrupt()
    release.set()
    await audio
    assert not any(e["type"] == "audio.delta" for e in voice.events)


async def test_speech_item_resume_keeps_turn_and_new_completed_item_gets_new_turn(voice):
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "resumed"})
    turn = voice.turn
    await voice.provider_event({"type": "input_audio_buffer.speech_stopped", "item_id": "resumed"})
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "resumed"})
    assert voice.turn == turn
    assert [e["turnId"] for e in voice.events if e["type"] == "turn.started"] == [turn, turn]
    await voice.provider_event(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "resumed",
            "transcript": "done",
        }
    )
    await voice.provider_event({"type": "input_audio_buffer.speech_stopped", "item_id": "resumed"})
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "resumed"})
    assert voice.turn != turn


@pytest.mark.parametrize("mode", ["text", "voice"])
@pytest.mark.parametrize("stage", ["generating", "awaiting_audio", "playing_buffer"])
async def test_each_output_stage_accepts_barge_in_and_keeps_microphone_uplink(voice, mode, stage):
    await created(voice, "old-stage")
    if stage == "awaiting_audio":
        await voice.provider_event(
            {"type": "response.text.done", "response_id": "old-stage", "text": "old answer"}
        )
    if stage == "playing_buffer":
        await voice.provider_event(
            {"type": "response.audio.delta", "response_id": "old-stage", "delta": "AAA="}
        )
        await voice.provider_event(
            {"type": "response.done", "response": {"id": "old-stage", "status": "completed"}}
        )
        assert "old-stage" in voice.playing and voice.idle.is_set()
    await voice.append_audio("AAA=")
    assert voice.sent[-1]["type"] == "input_audio_buffer.append"
    if mode == "text":
        await voice.input([{"type": "text", "text": "new request"}])
    else:
        await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "new-request"})
    boundary = max(i for i, e in enumerate(voice.events) if e["type"] == "playback.clear")
    await voice.provider_event({"type": "response.audio.delta", "response_id": "old-stage", "delta": "AAA="})
    await voice.append_audio("AAA=")
    assert voice.sent[-1]["type"] == "input_audio_buffer.append"
    assert not voice.playing
    assert not any(e["type"] == "audio.delta" for e in voice.events[boundary:])


async def test_streaming_asr_failure_and_item_cache_are_bounded(voice):
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "stream"})
    await voice.provider_event(
        {
            "type": "conversation.item.input_audio_transcription.text",
            "item_id": "stream",
            "text": "已识别",
            "stash": "暂存",
        }
    )
    assert voice.events[-1]["content"] == "已识别暂存" and voice.events[-1]["replace"]
    await voice.provider_event(
        {"type": "conversation.item.input_audio_transcription.failed", "item_id": "stream"}
    )
    assert voice.events[-1]["type"] == "transcript.discard"
    for index in range(120):
        voice.input_record(str(index))
    assert len(voice.input_records) == 100


@pytest.mark.parametrize("source", ["announcement", "blocked_automatic"])
async def test_every_cancel_path_recovers_lost_ack(voice, source):
    if source == "announcement":
        reply = Reply(voice.generation, origin="announcement")
        voice.replies[reply.request_id] = reply
        await created(voice, "announcement", {"audio_agent_gateway_request_id": reply.request_id})
        await voice.cancel_replies(lambda candidate: candidate is reply, "request_resolved")
    else:
        voice.input_enabled = False
        await created(voice, "blocked")
    assert voice.cancelling
    async with asyncio.timeout(0.5):
        while not voice.upstream.close.called:
            await asyncio.sleep(0.01)


async def test_late_previous_asr_does_not_clear_current_voice_item(voice):
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "previous"})
    await voice.provider_event({"type": "input_audio_buffer.speech_stopped", "item_id": "previous"})
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "current"})
    await voice.provider_event(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "previous",
            "transcript": "previous",
        }
    )
    assert voice.voice_start_item == "current"


async def test_failed_manual_submission_releases_voice_guard(voice, monkeypatch):
    monkeypatch.setattr(
        voice.gateway.tasks, "submit", AsyncMock(side_effect=ValueError("Task queue is full"))
    )
    await voice.input([{"type": "text", "text": "manual"}])
    assert any(e.get("content") == "Task queue is full" for e in voice.events)
    generation = voice.generation
    await voice.provider_event({"type": "input_audio_buffer.speech_started", "item_id": "retry-voice"})
    assert voice.generation == generation + 1
