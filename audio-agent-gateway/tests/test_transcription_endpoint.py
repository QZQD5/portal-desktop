from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway import voice as voice_module
from gateway.app import Gateway
from conftest import unit_settings as Settings
from gateway.voice import VoiceSession


@pytest.fixture
async def pending(tmp_path, monkeypatch):
    gateway = Gateway(Settings(data_dir=tmp_path, backend_enabled=False))
    session = VoiceSession(gateway, None, "endpoint", {"capabilities": ["input.audio"]})
    clock = SimpleNamespace(now=100.)
    monkeypatch.setattr(voice_module, "time", SimpleNamespace(monotonic=lambda: clock.now))
    session.emit = AsyncMock()
    session.upstream_send = AsyncMock()
    session.close_provider = AsyncMock()
    session.ready.set()
    session.playing.add("prior")
    for raw in [
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "utterance"},
        {"type": "conversation.item.input_audio_transcription.delta", "item_id": "utterance", "delta": "停"},
    ]:
        for event in session.protocol.normalize(raw):
            await session.provider_event(event)
    session.audio_activity = SimpleNamespace(speaking=False, quiet_seconds=.32, append=lambda _: False)
    session.input_records["utterance"]["audio_updated"] = clock.now = 105.
    session.emit.reset_mock()
    session.upstream_send.reset_mock()
    yield session, clock, session.input_records["utterance"]
    await session.close()
    await gateway.close()


@pytest.mark.parametrize("text", ["停", "好了", "帮我找昨天的文件", "删除桌面所有文件"])
async def test_quiet_timeout_requests_final_once_without_confirming_partial(pending, text):
    session, _, record = pending
    record["text"] = text
    for _ in range(2):
        await session.recover_stalled_transcription()
    session.upstream_send.assert_awaited_once_with({"type": "input_audio_buffer.commit"})
    assert not record["complete"] and not record.get("confirmed")
    assert session.ready.is_set() and session.store.history(session.id) == []
    assert session.queue.empty() and not session.gateway.tasks.tasks
    session.close_provider.assert_not_awaited()
    session.emit.assert_not_awaited()


async def test_commit_ack_is_not_final_and_audio_keeps_flowing(pending):
    session, clock, record = pending
    await session.recover_stalled_transcription()
    clock.now += .3
    await session.provider_event({"type": "input_audio_buffer.committed"})
    assert session.speaking_user and not record["complete"]
    await session.append_audio("AAAAAA==")
    session.upstream_send.assert_awaited_with({"type": "input_audio_buffer.append", "audio": "AAAAAA=="})
    clock.now += .3
    for _ in range(2):
        for event in session.protocol.normalize({
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "utterance", "text": "停。",
        }):
            await session.provider_event(event)
    await session.recover_stalled_transcription()
    history = session.store.history(session.id)
    assert len(history) == 1 and history[0]["content"] == "停。"
    assert not history[0].get("unconfirmed") and record["confirmed"]
    session.close_provider.assert_not_awaited()


async def test_only_missing_final_after_commit_reconnects(pending):
    session, clock, record = pending
    await session.recover_stalled_transcription()
    clock.now = 106.9
    record["audio_updated"] = clock.now
    await session.recover_stalled_transcription()
    session.close_provider.assert_not_awaited()
    # A partial update after the ACK does not replace the awaited final or extend its deadline.
    await session.provider_input({
        "type": "conversation.item.input_audio_transcription.delta", "item_id": "utterance", "text": "停",
    })
    await session.provider_event({"type": "input_audio_buffer.committed"})
    clock.now = 107.1
    record["audio_updated"] = clock.now
    await session.recover_stalled_transcription()
    session.close_provider.assert_awaited_once_with(reason="transcription_timeout")
    session.upstream_send.assert_awaited_once_with({"type": "input_audio_buffer.commit"})
    assert record["unconfirmed"] and not record.get("confirmed")
    assert session.queue.empty() and not session.gateway.tasks.tasks


async def test_local_speech_prevents_endpoint_even_when_partial_text_is_stale(pending):
    session, clock, record = pending
    clock.now = record["audio_updated"] = 120.
    session.audio_activity.speaking = True
    await session.recover_stalled_transcription()
    session.upstream_send.assert_not_awaited()
    session.close_provider.assert_not_awaited()
    assert not record["complete"]


async def test_new_asr_update_restarts_wait_even_during_silence(pending):
    session, clock, record = pending
    await session.provider_input({
        "type": "conversation.item.input_audio_transcription.delta", "item_id": "utterance", "text": "停一下",
    })
    clock.now = record["audio_updated"] = 109.9
    session.audio_activity.quiet_seconds = 9.9
    await session.recover_stalled_transcription()
    session.upstream_send.assert_not_awaited()
    session.close_provider.assert_not_awaited()
    clock.now = record["audio_updated"] = 110.
    await session.recover_stalled_transcription()
    session.upstream_send.assert_awaited_once_with({"type": "input_audio_buffer.commit"})


@pytest.mark.parametrize("text", ["停", ""])
async def test_identical_or_empty_asr_snapshots_do_not_restart_the_five_second_wait(pending, text):
    session, _, record = pending
    await session.provider_input({
        "type": "conversation.item.input_audio_transcription.delta", "item_id": "utterance", "text": text,
    })
    assert record["text_updated"] == 100.
    await session.recover_stalled_transcription()
    session.upstream_send.assert_awaited_once_with({"type": "input_audio_buffer.commit"})


async def test_new_partial_after_commit_restarts_wait_instead_of_reconnecting(pending):
    session, clock, record = pending
    await session.recover_stalled_transcription()
    clock.now = record["audio_updated"] = 106.
    await session.provider_input({
        "type": "conversation.item.input_audio_transcription.delta", "item_id": "utterance", "text": "停一下",
    })
    assert "endpoint_requested_at" not in record
    clock.now = record["audio_updated"] = 107.1
    await session.recover_stalled_transcription()
    session.close_provider.assert_not_awaited()
    assert session.upstream_send.await_count == 1
    clock.now = record["audio_updated"] = 111.
    await session.recover_stalled_transcription()
    assert session.upstream_send.await_count == 2


async def test_speech_resuming_after_commit_cancels_reconnect_deadline(pending):
    session, clock, record = pending
    await session.recover_stalled_transcription()
    clock.now = record["audio_updated"] = 107.1
    session.audio_activity.speaking = True
    await session.recover_stalled_transcription()
    assert "endpoint_requested_at" not in record and not record["complete"]
    session.close_provider.assert_not_awaited()
    # A delayed ACK must not mark the resumed speech as stopped.
    await session.provider_event({"type": "input_audio_buffer.committed"})
    assert session.speaking_user


async def test_local_onset_resets_endpoint_before_the_next_poll_even_if_speech_ends_in_packet(pending):
    session, clock, record = pending
    await session.recover_stalled_transcription()
    clock.now = 106.
    session.audio_activity.append = lambda _: True
    await session.append_audio("AAAAAA==")
    assert not session.audio_activity.speaking
    assert "endpoint_requested_at" not in record
    session.close_provider.assert_not_awaited()
    session.upstream_send.assert_awaited_with({"type": "input_audio_buffer.append", "audio": "AAAAAA=="})


@pytest.mark.parametrize("gap", [.6, 4.9, 5.1])
async def test_missing_audio_is_not_treated_as_confirmed_silence(pending, gap):
    session, clock, record = pending
    record["audio_updated"] = clock.now - gap
    await session.recover_stalled_transcription()
    session.upstream_send.assert_not_awaited()
    if gap < 5:
        session.close_provider.assert_not_awaited()
    else:
        session.close_provider.assert_awaited_once_with(reason="audio_input_timeout")


async def test_failed_commit_send_does_not_claim_endpoint_or_complete_partial(pending):
    session, _, record = pending
    session.upstream_send.side_effect = ConnectionError("disconnected")
    await session.recover_stalled_transcription()
    session.close_provider.assert_not_awaited()
    assert "endpoint_requested_at" not in record
    assert not record["complete"] and not record.get("confirmed")
    assert session.queue.empty() and not session.gateway.tasks.tasks
