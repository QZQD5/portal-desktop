import asyncio
import base64
import json
import struct
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import mock_text_delegation, open_client

from gateway.app import Gateway
from conftest import unit_settings as Settings
from gateway.conversation_context import snapshot
from gateway.seeduplex import (
    SeeduplexProtocol,
    classify_error,
    conversation_history,
    conversation_item,
    pcm16,
    session_config,
)
from gateway.voice import Reply, VoiceSession


def test_normalization_matches_executed_upstream_source():
    oracle = json.loads(
        (Path(__file__).parent / "fixtures/seeduplex-upstream-oracle.json").read_text("utf-8")
    )
    protocol = SeeduplexProtocol()
    for case in oracle["incoming"]:
        # The reference treats TTSEnded as nonterminal; live Seeduplex requires the fix below.
        if case["event"]["type"] == "response.output_audio.done":
            continue
        actual = protocol.normalize(case["event"])
        for event in actual:
            event.pop("batch_call_ids", None)
        assert actual == case["output"]
    for case in oracle["context"]:
        actual = conversation_item(case["item"])
        if case["item"]["type"] == "function_call_output":
            assert json.loads(actual["items"][0]["content"][0]["text"]) == {"error": True}
        assert actual == case["output"]


@pytest.mark.parametrize("samples", [[-1, -0.5, 0, 0.5, 1], [-2, 2]])
def test_float32_to_pcm16(samples):
    encoded = base64.b64encode(struct.pack("<" + "f" * len(samples), *samples)).decode()
    expected = [-32768, -16384, 0, 16384, 32767] if len(samples) == 5 else [-32768, 32767]
    assert list(struct.unpack("<" + "h" * len(samples), base64.b64decode(pcm16(encoded)))) == expected


@pytest.mark.parametrize("raw", [b"abc", struct.pack("<f", float("nan")), struct.pack("<f", float("inf"))])
def test_bad_audio_cannot_reach_browser(raw):
    with pytest.raises(ValueError):
        pcm16(base64.b64encode(raw).decode())


def test_session_matches_upstream_shape():
    s = session_config(
        Settings(),
        "prompt",
        [{"type": "function", "function": {"name": "task", "parameters": {"type": "object"}}}],
        "custom",
    )
    assert s == {
        "model": "1.2.6.1",
        "instructions": "prompt",
        "audio": {
            "input": {"format": {"type": "pcm", "rate": 16000}},
            "output": {
                "format": {"type": "pcm", "rate": 24000},
                "voice": "custom",
                "speed": 0,
                "loudness": 0,
            },
        },
        "tools": [{"type": "function", "name": "task", "parameters": {"type": "object"}}],
    }


def test_late_old_id_cannot_steal_new_response_slot():
    p = SeeduplexProtocol()
    p.normalize({"type": "response.output_text.delta", "response_id": "old", "text": "old"})
    p.normalize({"type": "response.canceled"})
    p.normalize({"type": "response.output_text.delta", "response_id": "new", "text": "new"})
    assert (
        p.normalize({"type": "response.output_audio.delta", "response_id": "old", "audio": "AAAAAA=="}) == []
    )
    assert p.active == "new"
    assert p.normalize({"type": "response.done"})[-1]["response"]["id"] == "new"


@pytest.mark.parametrize(
    "message, expected",
    [
        ("Invalid X-Api-Key", "fatal"),
        ("HTTP 403", "fatal"),
        ("quota exceeded", "fatal"),
        ("no response to cancel", "no_active_response"),
        ("no session active, please create a session first", "no_active_session"),
        ("audio idle timeout", "inactivity"),
        ("session was closed because no response was generated for 60 seconds", "inactivity"),
        ("content safety", "content_safety"),
        ("server busy", "other"),
    ],
)
def test_error_classification(message, expected):
    assert classify_error(message) == expected


@pytest.fixture
async def native(tmp_path):
    gateway = Gateway(Settings(data_dir=tmp_path, backend_enabled=False))
    mock_text_delegation(gateway)
    session = VoiceSession(gateway, None, "native", {"capabilities": ["input.audio"]})
    session.emit = AsyncMock()
    session.upstream_send = AsyncMock()
    session.ready.set()
    yield session
    await session.close()
    await gateway.close()


async def test_audio_transcript_deltas_reach_desktop_before_playback_or_final(native):
    await native.provider_event({"type": "response.created", "response": {"id": "streaming"}})
    await native.provider_event({"type": "response.output_audio_transcript.delta", "response_id": "streaming", "delta": "第一段"})
    await native.provider_event({"type": "response.output_audio_transcript.delta", "response_id": "streaming", "delta": "第二段"})
    events = [call.args[0] for call in native.emit.call_args_list]
    assert [e["content"] for e in events if e["type"] == "transcript.delta"] == ["第一段", "第二段"]
    assert not any(e["type"] == "transcript.final" for e in events)
    assert not native.responses["streaming"].get("playback_started")


@pytest.mark.parametrize("cloud_start", [False, True])
async def test_idle_speech_without_transcription_recovers_and_is_not_an_interruption(native, cloud_start):
    native.close_provider = AsyncMock()
    pcm = (Path(__file__).parent / "fixtures/speech-stop.pcm").read_bytes()
    await native.append_audio(base64.b64encode(pcm).decode())
    assert native.local_barge_in and not native.local_barge_in["interrupted_output"]
    assert not any(c.args[0]["type"] == "playback.clear" for c in native.emit.call_args_list)
    record = native.local_barge_in
    if cloud_start:
        for event in native.protocol.normalize({
            "type": "conversation.item.input_audio_transcription.started", "item_id": "empty-asr",
        }):
            await native.provider_event(event)
        record = native.input_records["empty-asr"]
        assert not record["interrupted_output"]
    record["started_at"] -= 6
    native.audio_activity.speaking = False
    native.audio_activity.quiet_frames = 30
    await native.recover_missing_barge_in_transcription()
    assert "endpoint_requested_at" in record
    record["endpoint_requested_at"] -= 2.1
    await native.recover_missing_barge_in_transcription()
    native.close_provider.assert_awaited_once_with(reason="missing_transcription")
    assert not native.speaking_user and not native.native_pending_since
    assert not native.gateway.tasks.tasks
    if cloud_start:
        assert record["invalid"] and not record.get("confirmed")


async def test_local_detection_after_cloud_start_does_not_create_second_missing_asr_timer(native):
    for event in native.protocol.normalize({
        "type": "conversation.item.input_audio_transcription.started", "item_id": "early-asr",
    }):
        await native.provider_event(event)
    await native.stop_playback_on_speech()
    assert native.local_barge_in is None
    assert native.input_records["early-asr"]["started"]


@pytest.mark.parametrize("blocked", ["input_enabled", "sleeping", "suspended", "manual"])
async def test_missing_asr_monitor_respects_input_controls(native, blocked, monkeypatch):
    native.close_provider = AsyncMock()
    native.local_barge_in = {"started_at": time.monotonic() - 30, "endpoint_requested_at": 1}
    native.audio_activity.quiet_frames = 30
    if blocked == "suspended":
        monkeypatch.setattr(native.gateway, "input_status", lambda: {"suspended": True})
    elif blocked == "manual":
        native.manual_generation = native.generation
    else:
        setattr(native, blocked, blocked != "input_enabled")
    await native.recover_missing_barge_in_transcription()
    native.close_provider.assert_not_awaited()
    native.upstream_send.assert_not_awaited()


async def test_speech_audio_deadlines_ignore_text_activity_and_accept_fresh_audio(native, monkeypatch):
    clock = SimpleNamespace(now=100.)
    monkeypatch.setattr("gateway.voice.time", SimpleNamespace(monotonic=lambda: clock.now))
    reply = Reply(native.generation, requested_at=100., speech_text="已返回的结果")
    native.replies[reply.request_id] = reply
    native.request_generations[reply.request_id] = native.generation
    await native.provider_event({"type": "response.created", "response": {
        "id": "tts", "metadata": {"audio_agent_gateway_request_id": reply.request_id},
    }})
    clock.now = 114.
    assert not native.reply_stalled(reply)
    clock.now = 116.
    await native.provider_event({"type": "response.output_text.delta", "response_id": "tts", "delta": "文字仍在更新"})
    assert native.reply_stall_reason(reply) == "first_audio_timeout"
    await native.provider_event({"type": "response.audio.delta", "response_id": "tts", "delta": "AAAA"})
    assert not native.reply_stalled(reply)
    clock.now = 135.
    assert not native.reply_stalled(reply)
    clock.now = 137.
    await native.provider_event({"type": "response.output_text.delta", "response_id": "tts", "delta": "仍有文字"})
    assert native.reply_stall_reason(reply) == "audio_inactivity_timeout"
    await native.provider_event({"type": "response.audio.delta", "response_id": "tts", "delta": "AAAA"})
    assert not native.reply_stalled(reply)


async def test_live_asr_fields_create_turn_and_preserve_final_recognition(native):
    raw = [
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "q1"},
        {"type": "conversation.item.input_audio_transcription.delta", "item_id": "q1", "delta": "你是谁"},
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "q1",
            "text": "你是谁？",
        },
    ]
    for event in raw:
        for normalized in native.protocol.normalize(event):
            await native.provider_event(normalized)
    assert native.generation == 1 and not native.speaking_user
    assert native.store.history(native.id)[-1]["content"] == "你是谁？"
    emitted = [call.args[0] for call in native.emit.call_args_list]
    assert any(e["type"] == "playback.clear" for e in emitted)
    assert any(e["type"] == "transcript.delta" and e["content"] == "你是谁" and e["replace"] for e in emitted)
    # Late duplicate starts and completions must not interrupt the next utterance.
    for event in native.protocol.normalize(
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "q2"}
    ):
        await native.provider_event(event)
    for event in [raw[0], raw[-1]]:
        for normalized in native.protocol.normalize(event):
            await native.provider_event(normalized)
    assert native.generation == 2 and native.speaking_user


@pytest.mark.parametrize("text", ["到哪一步了", "删除桌面所有文件"])
async def test_stalled_asr_preserves_question_and_never_executes_partial_commands(native, text):
    native.close_provider = AsyncMock()
    native.tools.execute = AsyncMock()
    native.gateway.tasks.tasks["queued"] = {
        "id": "queued", "sessionId": native.id, "createdAt": 1, "status": "queued",
        "objective": "Blender建模",
    }
    for raw in [
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "stalled"},
        {"type": "conversation.item.input_audio_transcription.delta", "item_id": "stalled", "delta": text},
    ]:
        for event in native.protocol.normalize(raw):
            await native.provider_event(event)
    await native.recover_stalled_transcription()
    assert native.queue.empty()
    native.input_records["stalled"]["text_updated"] = time.monotonic() - 4.9
    await native.recover_stalled_transcription()
    assert not native.input_records["stalled"].get("awaiting_confirmation")
    assert not native.input_records["stalled"]["complete"]
    assert native.ready.is_set() and native.queue.empty()
    native.close_provider.assert_not_awaited()
    assert native.store.history(native.id) == []
    native.input_records["stalled"]["text_updated"] = time.monotonic() - 5.1
    native.input_records["stalled"]["audio_updated"] = time.monotonic()
    await native.recover_stalled_transcription()
    native.close_provider.assert_not_awaited()
    native.input_records["stalled"]["endpoint_requested_at"] -= 2.1
    await native.recover_stalled_transcription()
    reply = native.queue.get_nowait()
    assert "请再说一次" in reply.speech_text
    assert native.store.history(native.id)[0]["unconfirmed"]
    assert snapshot(native.store, {}, native.id)["messages"] == []
    assert not native.speaking_user and native.native_pending_since == 0
    native.close_provider.assert_awaited_once_with(reason="transcription_timeout")
    native.tools.execute.assert_not_awaited()
    # A late final from the stalled turn cannot replace the retained question.
    for event in native.protocol.normalize({
        "type": "conversation.item.input_audio_transcription.completed",
        "item_id": "stalled", "text": "取消任务。",
    }):
        await native.provider_event(event)
    assert native.store.history(native.id)[0]["content"] == text
    await native.recover_stalled_transcription()
    assert native.queue.empty()
    native.gateway.tasks.tasks.clear()


async def test_final_asr_before_timeout_confirms_once(native):
    native.close_provider = AsyncMock()
    for raw in [
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "late"},
        {"type": "conversation.item.input_audio_transcription.delta", "item_id": "late", "delta": "整理文件"},
    ]:
        for event in native.protocol.normalize(raw):
            await native.provider_event(event)
    record = native.input_records["late"]
    record["text_updated"] = time.monotonic() - 4.9
    await native.recover_stalled_transcription()
    assert not record["complete"] and not record["invalid"]
    assert not native.gateway.tasks.tasks
    for _ in range(2):
        for event in native.protocol.normalize({
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "late", "text": "整理下载目录。",
        }):
            await native.provider_event(event)
    assert record["confirmed"] and not record["awaiting_confirmation"]
    assert [m["content"] for m in native.store.history(native.id)] == ["整理下载目录。"]
    assert not native.store.history(native.id)[0].get("unconfirmed")
    await native.recover_interrupted_request(record)
    await native.recover_interrupted_request(record)
    tasks = list(native.gateway.tasks.tasks.values())
    assert tasks == []
    native.close_provider.assert_not_awaited()
    assert native.queue.empty()


async def test_unchanged_asr_snapshot_does_not_delay_endpoint(native):
    native.close_provider = AsyncMock()
    raw = {"type": "conversation.item.input_audio_transcription.delta", "item_id": "same", "delta": "好了"}
    for event in native.protocol.normalize(raw):
        await native.provider_event(event)
    record = native.input_records["same"]
    record.update(text_updated=time.monotonic() - 9, awaiting_confirmation=True)
    record["audio_updated"] = time.monotonic()
    for event in native.protocol.normalize(raw):
        await native.provider_event(event)
    await native.recover_stalled_transcription()
    assert record.get("endpoint_requested_at") and not record["complete"]
    native.close_provider.assert_not_awaited()
    assert native.queue.empty()


@pytest.mark.parametrize("playback", ["generating", "buffered"])
@pytest.mark.parametrize("next_speech", [False, True])
@pytest.mark.parametrize("text", ["好了", "停", "删除桌面所有文件"])
async def test_unconfirmed_barge_in_recovers_without_speaking(native, playback, next_speech, text):
    native.close_provider = AsyncMock()
    if playback == "generating":
        await native.provider_event({"type": "response.created", "response": {"id": "prior"}})
    else:
        native.playing.add("prior")
    for raw in [
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "stop"},
        {"type": "conversation.item.input_audio_transcription.delta", "item_id": "stop", "delta": text},
    ]:
        for event in native.protocol.normalize(raw):
            await native.provider_event(event)
    record = native.input_records["stop"]
    assert record["interrupted_output"]
    assert not native.active and not native.playing
    if next_speech:
        for event in native.protocol.normalize({
            "type": "conversation.item.input_audio_transcription.started", "item_id": "next",
        }):
            await native.provider_event(event)
        native.close_provider.assert_not_awaited()
    else:
        record["text_updated"] = time.monotonic() - 5.1
        record["audio_updated"] = time.monotonic()
        await native.recover_stalled_transcription()
        native.close_provider.assert_not_awaited()
        record["endpoint_requested_at"] -= 2.1
        await native.recover_stalled_transcription()
        native.close_provider.assert_awaited_once_with(reason="transcription_timeout")
    await native.recover_interrupted_request(record)
    assert record["unconfirmed"] and record["handled"]
    history = native.store.history(native.id)
    assert len(history) == 1 and history[0]["content"] == text and history[0]["unconfirmed"]
    assert not native.gateway.tasks.tasks and not record.get("confirmed")
    assert native.queue.empty()
    assert not any(call.args[0].get("role") == "assistant" for call in native.emit.call_args_list)


async def test_stalled_barge_in_logs_audio_continuity_without_audio_content(native, caplog):
    native.close_provider = AsyncMock()
    native.playing.add("prior")
    for raw in [
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "stop"},
        {"type": "conversation.item.input_audio_transcription.delta", "item_id": "stop", "delta": "停"},
    ]:
        for event in native.protocol.normalize(raw):
            await native.provider_event(event)
    record = native.input_records["stop"]
    await native.append_audio("AAAAAA==")
    assert record["audio_bytes_after_start"] == 4
    assert record["audio_updated"] > 0
    record["text_updated"] = time.monotonic() - 5.1
    await native.recover_stalled_transcription()
    record["endpoint_requested_at"] -= 2.1
    record["audio_updated"] = time.monotonic() - 5.1
    await native.recover_stalled_transcription()
    assert native.queue.empty() and not native.gateway.tasks.tasks
    assert "audio_bytes_after_start=4 audio_gap_ms=" in caplog.text
    assert "AAAAAA==" not in caplog.text


@pytest.mark.parametrize("generating", [False, True])
async def test_local_speech_stops_output_before_cloud_asr_without_cancelling_input(native, generating):
    await native.provider_event({"type": "response.created", "response": {"id": "old"}})
    await native.provider_event({"type": "response.audio.delta", "response_id": "old", "delta": "AAAA"})
    if not generating:
        await native.provider_event({"type": "response.done", "response_id": "old"})
    native.emit.reset_mock()
    native.upstream_send.reset_mock()
    generation = native.generation
    pcm = (Path(__file__).parent / "fixtures/speech-stop.pcm").read_bytes()
    for offset in range(0, len(pcm), 86):
        await native.append_audio(base64.b64encode(pcm[offset:offset + 86]).decode())
    assert native.generation == generation and native.local_barge_in is None
    assert not native.responses["old"].get("playback_ended")
    assert sum(c.args[0]["type"] == "playback.clear" for c in native.emit.call_args_list) == 0
    sent_audio = b"".join(base64.b64decode(c.args[0]["audio"]) for c in native.upstream_send.call_args_list)
    assert sent_audio == pcm  # No cancel, mute, truncation, or fabricated ASR boundary.
    for raw in [
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "fresh"},
        {"type": "conversation.item.input_audio_transcription.completed", "item_id": "fresh", "text": "停。"},
    ]:
        for event in native.protocol.normalize(raw):
            await native.provider_event(event)
    assert native.input_records["fresh"]["interrupted_output"]
    assert native.input_records["fresh"]["confirmed"]
    assert native.generation == generation + 1
    await native.provider_event({"type": "response.created", "response": {"id": "fresh-reply"}})
    assert not native.responses["fresh-reply"]["suppressed"]


@pytest.fixture
async def short_interjection_during_playback(native):
    await native.provider_event({"type": "response.created", "response": {"id": "old"}})
    await native.provider_event({"type": "response.audio.delta", "response_id": "old", "delta": "AAAA"})
    await native.playback_started("old")
    # Use the shipped Silero model, then let it observe silence before the cloud onset.
    pcm = (Path(__file__).parent / "fixtures/speech-stop.pcm").read_bytes() + bytes(18 * 1024)
    heard_speech = False
    for offset in range(0, len(pcm), 1280):
        await native.append_audio(base64.b64encode(pcm[offset:offset + 1280]).decode())
        heard_speech |= native.audio_activity.speaking
    assert heard_speech and not native.audio_activity.speaking and "old" in native.playing
    return native


@pytest.mark.parametrize("partial,final", [
    (None, None), ("等一下", "等一下。"), (None, "等一下。"), ("你继续", "你继续。"),
], ids=["onset-only", "stop-partial", "stop-final", "ordinary-request"])
async def test_late_native_onset_waits_for_transcript(short_interjection_during_playback, partial, final):
    session = short_interjection_during_playback
    generation = session.generation
    asr = "conversation.item.input_audio_transcription."
    packets = [({"type": asr + "started"}, False)]
    if partial:
        packets.append(({"type": asr + "delta", "delta": partial}, True))
    if final:
        packets.append(({"type": asr + "completed", "text": final}, True))

    for packet, interrupted in packets:
        for event in session.protocol.normalize({**packet, "item_id": "late"}):
            await session.provider_event(event)
        clears = [call for call in session.emit.call_args_list if call.args[0]["type"] == "playback.clear"]
        assert len(clears) == int(interrupted)
        assert ("old" in session.playing) == (not interrupted)
        assert session.generation == generation + int(interrupted)
    if final:
        assert session.input_records["late"]["confirmed"]
        assert any(m["role"] == "user" and m["content"] == final for m in session.store.history(session.id))


async def test_short_interjection_does_not_defer_onset_during_next_reply(short_interjection_during_playback):
    session = short_interjection_during_playback
    await session.provider_event({"type": "response.done", "response_id": "old"})
    await session.playback_ended("old")
    await session.provider_event({"type": "response.created", "response": {"id": "new"}})
    await session.provider_event({"type": "response.audio.delta", "response_id": "new", "delta": "AAAA"})
    await session.playback_started("new")
    for event in session.protocol.normalize({"type": "conversation.item.input_audio_transcription.started", "item_id": "next"}):
        await session.provider_event(event)
    assert session.input_records["next"]["started"] and "new" not in session.playing
    assert any(call.args[0]["type"] == "playback.clear" for call in session.emit.call_args_list)


@pytest.mark.parametrize("playing", [False, True])
@pytest.mark.parametrize("queued", [False, True])
async def test_local_vad_delivers_waiting_results_as_text_without_replaying_or_cancelling_work(native, playing, queued):
    native.settings.announcement_batch_window = 0
    if playing:
        config = await native.gateway.tasks.submit("查看电脑配置", native.id)
        config.update(status="completed", result="电脑配置已查到。", notificationStatus="pending")
        await native.gateway.tasks.publish(config, "task.completed")
        await native.queue_announcements()
        current = native.queue.get_nowait()
        native.queue.task_done()
        native.replies[current.request_id] = current
        await native.provider_event({"type": "response.created", "response": {
            "id": "config", "metadata": {"audio_agent_gateway_request_id": current.request_id},
        }})
        await native.provider_event({"type": "response.audio.delta", "response_id": "config", "delta": "AAAA"})
        await native.provider_event({"type": "response.done", "response_id": "config"})
    memory = await native.gateway.tasks.submit("检查内存", native.id)
    memory.update(status="completed", result="内存实时读数：24.66 GB。", notificationStatus="pending")
    await native.gateway.tasks.publish(memory, "task.completed")
    work = await native.gateway.tasks.submit("继续处理报告", native.id)
    reply = None
    if queued:
        await native.queue_announcements()
        reply = native.queue.get_nowait()
        native.queue.task_done()
        native.replies[reply.request_id] = reply

    native.emit.reset_mock()
    native.upstream_send.reset_mock()
    pcm = (Path(__file__).parent / "fixtures/speech-stop.pcm").read_bytes()
    await native.append_audio(base64.b64encode(pcm).decode())
    if playing and not native.local_barge_in:
        await native.stop_playback_on_speech()
    await asyncio.gather(*native.background)
    assert memory["notificationStatus"] == "delivered"
    assert work["status"] == "queued"
    assert not native.playing
    events = [c.args[0] for c in native.emit.call_args_list]
    assert any(e["type"] == "playback.clear" for e in events)
    assert [e["content"] for e in events if e["type"] == "transcript.final"] == [memory["result"]]
    assert len([m for m in native.gateway.history.page() if m["content"] == memory["result"]]) == 1
    assert [c.args[0]["type"] for c in native.upstream_send.call_args_list
            if c.args[0]["type"] in {"input_audio_buffer.append", "response.cancel"}] == ["input_audio_buffer.append"]
    if reply:
        assert reply.done.is_set()
        await native.provider_event({"type": "response.created", "response": {
            "id": "late-result", "metadata": {"audio_agent_gateway_request_id": reply.request_id},
        }})
        assert native.responses["late-result"]["suppressed"]

    for event in native.protocol.normalize({
        "type": "conversation.item.input_audio_transcription.started", "item_id": "fresh",
    }):
        await native.provider_event(event)
    await native.queue_announcements()
    assert native.queue.empty()
    work.update(status="completed", result="新任务完成", notificationStatus="pending")
    await native.queue_announcements()
    assert native.queue.get_nowait().delivery_tasks == [work["id"]]


async def test_slow_result_delivery_cannot_block_audio_upload_after_local_barge_in(native):
    await native.provider_event({"type": "response.created", "response": {"id": "old"}})
    await native.provider_event({"type": "response.audio.delta", "response_id": "old", "delta": "AAAA"})
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow_delivery(tasks):
        entered.set()
        await release.wait()

    native.text_announcements = AsyncMock(side_effect=slow_delivery)
    native.upstream_send.reset_mock()
    pcm = (Path(__file__).parent / "fixtures/speech-stop.pcm").read_bytes()
    try:
        await asyncio.wait_for(native.append_audio(base64.b64encode(pcm).decode()), 1)
        if not entered.is_set():
            await native.stop_playback_on_speech()
        await asyncio.wait_for(entered.wait(), 1)
        native.emit.assert_any_await({"type": "playback.clear", "reason": "user_interruption"})
        await asyncio.wait_for(native.append_audio("AAAAAA=="), 1)
        sent = [c.args[0] for c in native.upstream_send.call_args_list]
        assert [e["type"] for e in sent] == ["input_audio_buffer.append"] * 2
        assert b"".join(base64.b64decode(e["audio"]) for e in sent) == pcm + bytes(4)
        assert native.ready.is_set() and native.responses["old"]["playback_ended"]
    finally:
        release.set()
        await asyncio.gather(*native.background)


@pytest.mark.parametrize("outcome", ["sent", "failed", "obsolete"])
async def test_barge_in_without_asr_logs_real_audio_writes_once(native, outcome, caplog):
    class Socket:
        transport = None
        close = AsyncMock()

        async def send(self, payload):
            if outcome == "failed":
                raise ConnectionError("test send failure")

    native.upstream = Socket()
    native.upstream_send = VoiceSession.upstream_send.__get__(native)
    await native.provider_event({"type": "response.created", "response": {"id": "old"}})
    await native.stop_playback_on_speech()
    record = native.local_barge_in
    if outcome == "obsolete":
        native.closing_upstream = native.upstream
    audio = base64.b64encode(struct.pack('<4h', 100, -200, 300, -400)).decode()
    await native.append_audio(audio)
    await asyncio.gather(*native.upstream_send_tasks)
    await asyncio.sleep(0)
    native.log_missing_barge_in_transcription()
    assert "realtime.barge_in_no_transcription" not in caplog.text
    record["started_at"] -= 5.1
    for _ in range(2):
        native.log_missing_barge_in_transcription()
    assert caplog.text.count("realtime.barge_in_no_transcription") == 1
    assert f"audio_bytes=8 audio_sent_bytes={8 if outcome == 'sent' else 0}" in caplog.text
    assert "peak_pcm=400 max_rms_pcm=273.9" in caplog.text
    assert audio not in caplog.text
    assert not native.input_records and not native.gateway.tasks.tasks
    assert native.queue.empty()


async def test_missing_asr_recovery_endpoints_then_reconnects_without_inventing_input(native):
    native.close_provider = AsyncMock()
    native.local_barge_in = {"started_at": time.monotonic() - 6}
    native.audio_activity.quiet_frames = 30
    await native.recover_missing_barge_in_transcription()
    native.upstream_send.assert_awaited_once_with({"type": "input_audio_buffer.commit"})
    await native.recover_missing_barge_in_transcription()
    native.close_provider.assert_not_awaited()
    native.local_barge_in["endpoint_requested_at"] -= 2.1
    await native.recover_missing_barge_in_transcription()
    native.close_provider.assert_awaited_once_with(reason="missing_transcription")
    assert not native.ready.is_set() and native.local_barge_in is None
    assert not native.input_records and not native.gateway.tasks.tasks
    assert not any(c.args[0]["type"].startswith("transcript.") for c in native.emit.call_args_list)
    assert native.queue.empty()


async def test_missing_asr_recovery_waits_for_speech_and_cloud_ack(native):
    native.close_provider = AsyncMock()
    native.local_barge_in = {"started_at": time.monotonic() - 30, "endpoint_requested_at": 1}
    native.audio_activity.speaking = True
    await native.recover_missing_barge_in_transcription()
    assert "endpoint_requested_at" not in native.local_barge_in
    native.upstream_send.assert_not_awaited()
    native.audio_activity.speaking = False
    native.audio_activity.quiet_frames = 30
    await native.recover_missing_barge_in_transcription()
    native.audio_activity.speaking = True
    await native.recover_missing_barge_in_transcription()
    assert "endpoint_requested_at" not in native.local_barge_in
    native.close_provider.assert_not_awaited()
    native.audio_activity.speaking = False
    await native.recover_missing_barge_in_transcription()
    for event in native.protocol.normalize({
        "type": "conversation.item.input_audio_transcription.started", "item_id": "ack",
    }):
        await native.provider_event(event)
    assert native.local_barge_in is None
    await native.recover_missing_barge_in_transcription()
    native.close_provider.assert_not_awaited()


async def test_missing_asr_endpoint_send_race_does_not_stop_monitor(native):
    native.local_barge_in = {"started_at": time.monotonic() - 6}
    native.audio_activity.quiet_frames = 30
    native.upstream_send.side_effect = ConnectionError("provider disconnected")
    await native.recover_missing_barge_in_transcription()
    assert "endpoint_requested_at" not in native.local_barge_in
    native.upstream_send.side_effect = None
    await native.recover_missing_barge_in_transcription()
    assert "endpoint_requested_at" in native.local_barge_in
    assert not any(c.args[0]["type"].startswith("transcript.") for c in native.emit.call_args_list)
    for event in native.protocol.normalize({
        "type": "conversation.item.input_audio_transcription.started", "item_id": "fresh",
    }):
        await native.provider_event(event)
    assert native.local_barge_in is None


@pytest.mark.parametrize("release", ["asr", "quiet", "audio_lost"])
async def test_local_barge_in_defers_queued_speech_until_asr_or_quiet(native, release):
    await native.provider_event({"type": "response.created", "response": {"id": "old"}})
    await native.provider_event({"type": "response.audio.delta", "response_id": "old", "delta": "AAAA"})
    await native.provider_event({"type": "response.done", "response_id": "old"})
    await native.stop_playback_on_speech()
    assert not native.playing and native.active is None and not native.speaking_user
    native.audio_activity = SimpleNamespace(quiet_seconds=.2)
    native.local_barge_in.update(started_at=time.monotonic() - 30, audio_updated=time.monotonic(), logged=True)
    reply = Reply(native.generation, origin="announcement", speech_text="后台结果")
    waiter = asyncio.create_task(native.wait_response_slot(reply))
    try:
        await asyncio.sleep(.08)
        assert not waiter.done()
        if release == "asr":
            for event in native.protocol.normalize({
                "type": "conversation.item.input_audio_transcription.started", "item_id": "fresh",
            }):
                await native.provider_event(event)
            assert not await asyncio.wait_for(waiter, 1)
            assert native.speaking_user and native.ready.is_set()
        else:
            if release == "quiet":
                native.audio_activity.quiet_seconds = 5.1
            else:
                native.local_barge_in["audio_updated"] = time.monotonic() - 5.1
            assert await asyncio.wait_for(waiter, 1)
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.parametrize("cloud_asr", [False, True])
async def test_waiting_or_retired_reply_cannot_reconnect_during_user_speech(native, cloud_asr):
    native.upstream = SimpleNamespace(close=AsyncMock())
    native.settings.response_timeout = .01
    native.idle.clear()
    if cloud_asr:
        native.speaking_user = True
    else:
        native.local_barge_in = {"started_at": time.monotonic()}
    reply = Reply(native.generation, origin="announcement", speech_text="旧结果")
    waiter = asyncio.create_task(native.wait_response_slot(reply))
    try:
        await asyncio.sleep(.04)
        assert not waiter.done()
        native.upstream.close.assert_not_awaited()
        reply.done.set()
        assert not await asyncio.wait_for(waiter, .1)
        native.upstream.close.assert_not_awaited()
    finally:
        native.upstream = None
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.parametrize("outcome", ["sent", "failed", "obsolete"])
async def test_audio_diagnostics_count_completed_writes_for_the_captured_turn(native, outcome):
    class Socket:
        transport = None
        close = AsyncMock()

        async def send(self, payload):
            if outcome == "failed":
                raise ConnectionError("test send failure")

    native.upstream = Socket()
    native.upstream_send = VoiceSession.upstream_send.__get__(native)
    record = native.input_record("first")
    native.voice_start_item = "first"
    await native.send_lock.acquire()
    try:
        await native.append_audio("AAAAAA==")
        assert record["audio_bytes_after_start"] == 4
        assert not record.get("audio_sent_bytes")
        native.voice_start_item = "second"
        second = native.input_record("second")
        if outcome == "obsolete":
            native.closing_upstream = native.upstream
    finally:
        native.send_lock.release()
    await asyncio.gather(*native.upstream_send_tasks)
    await asyncio.sleep(0)
    assert record.get("audio_sent_bytes", 0) == (4 if outcome == "sent" else 0)
    assert not second.get("audio_sent_bytes")
    assert not native.upstream_pending_bytes
    native.upstream = None


@pytest.mark.parametrize("has_start", [True, False])
async def test_new_speech_preserves_previous_unconfirmed_transcript(native, has_start):
    raw_events = [
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "query"},
        {"type": "conversation.item.input_audio_transcription.delta", "item_id": "query", "delta": "到哪一步了"},
    ]
    if has_start:
        raw_events.append({"type": "conversation.item.input_audio_transcription.started", "item_id": "cancel"})
    raw_events += [
        {"type": "conversation.item.input_audio_transcription.delta", "item_id": "cancel", "delta": "取消任务"},
        {"type": "conversation.item.input_audio_transcription.completed", "item_id": "cancel", "text": "取消任务。"},
    ]
    for raw in raw_events:
        for event in native.protocol.normalize(raw):
            await native.provider_event(event)
    emitted = [call.args[0] for call in native.emit.call_args_list]
    transcripts = [e for e in emitted if e["type"].startswith("transcript.") and e.get("role") == "user"]
    assert [e["content"] for e in transcripts] == ["到哪一步了", "到哪一步了", "取消任务", "取消任务。"]
    assert transcripts[1]["type"] == "transcript.final" and transcripts[1]["unconfirmed"]
    assert transcripts[0]["turnId"] != transcripts[2]["turnId"]
    history = [m for m in native.store.history(native.id) if m["role"] == "user"]
    assert [m["content"] for m in history] == ["到哪一步了", "取消任务。"]
    assert history[0]["unconfirmed"]
    assert history[0]["turnId"] != history[1]["turnId"]
    assert [m["content"] for m in snapshot(native.store, {}, native.id)["messages"] if m["role"] == "user"] == ["取消任务。"]


async def test_final_asr_ignores_late_replacement_deltas(native):
    for raw in [
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "query"},
        {"type": "conversation.item.input_audio_transcription.completed", "item_id": "query", "text": "到哪一步了"},
    ]:
        for event in native.protocol.normalize(raw):
            await native.provider_event(event)
    native.emit.reset_mock()
    for event in native.protocol.normalize({
        "type": "conversation.item.input_audio_transcription.delta", "item_id": "query", "delta": "取消任务",
    }):
        await native.provider_event(event)
    native.emit.assert_not_awaited()
    assert native.store.history(native.id)[0]["content"] == "到哪一步了"


async def test_live_unidentified_tool_then_identified_audio_keeps_one_reply(native):
    native.tools.execute = AsyncMock(return_value={"time": "12:00"})
    for event in native.protocol.normalize(
        {
            "type": "response.function_call_arguments.done",
            "items": [{"name": "clock", "call_id": "c", "arguments": "{}"}],
        }
    ):
        await native.provider_event(event)
    local_id = native.active
    await asyncio.gather(*native.tool_tasks)
    for raw in [
        {"type": "response.output_text.delta", "response_id": "wire", "delta": "十二点"},
        {"type": "response.output_text.done", "response_id": "wire", "text": "十二点"},
        {"type": "response.output_audio.delta", "response_id": "wire", "delta": "AAAAAA=="},
        {"type": "response.output_audio.done", "response_id": "wire"},
    ]:
        for event in native.protocol.normalize(raw):
            await native.provider_event(event)
    assert native.idle.is_set() and native.active is None
    assert native.responses[local_id]["done"]
    assert native.store.history(native.id)[-1]["content"] == "十二点"
    assert len(native.responses) == 1
    assert (
        native.protocol.normalize(
            {"type": "response.output_audio.delta", "response_id": "wire", "delta": "AAAAAA=="}
        )
        == []
    )


def test_delayed_usage_cannot_finish_new_response():
    protocol = SeeduplexProtocol()
    protocol.normalize({"type": "response.output_text.delta", "response_id": "old", "delta": "旧"})
    ended = protocol.normalize({"type": "response.output_audio.done", "response_id": "old"})
    assert ended[-1]["type"] == "response.done" and protocol.active is None
    protocol.normalize({"type": "response.output_text.delta", "response_id": "new", "delta": "新"})
    usage = protocol.normalize({"type": "response.done", "response": {"usage": {"total_tokens": 10}}})
    assert usage[0]["type"] == "response.usage" and protocol.active == "new"


async def test_native_barge_in_does_not_cancel_the_new_recognition(native):
    for event in native.protocol.normalize(
        {"type": "response.output_text.delta", "response_id": "old", "delta": "旧回复"}
    ):
        await native.provider_event(event)
    old = native.active
    await native.playback_started(old)
    for event in native.protocol.normalize(
        {"type": "conversation.item.input_audio_transcription.started", "item_id": "new"}
    ):
        await native.provider_event(event)
    assert native.speaking_user and native.active is None and native.idle.is_set()
    message = next(m for m in native.store.history(native.id) if m["id"] == "voice:" + old)
    assert message["playbackStatus"] == "interrupted" and message["partialTranscript"]
    await asyncio.sleep(0)
    assert not native.upstream_send.called
    assert (
        native.protocol.normalize(
            {"type": "response.output_audio.delta", "response_id": "old", "delta": "AAAAAA=="}
        )
        == []
    )


def test_reconnect_history_sends_complete_pairs_only():
    event = conversation_history(
        [
            {"role": "assistant", "content": "orphan"},
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "answer"},
            {"role": "user", "content": "not answered"},
        ]
    )
    assert [item["role"] for item in event["items"]] == ["user", "assistant"]
    assert [item["content"][0]["text"] for item in event["items"]] == ["question", "answer"]
    assert conversation_history([{"role": "user", "content": "pending"}]) is None


async def test_background_context_is_a_pair_and_speech_uses_actual_result(native):
    native.settings.announcement_batch_window = 0
    task = await native.gateway.tasks.submit("test task", native.id)
    task.update(status="completed", result="实际结果是四十二。", notificationStatus="pending")
    await native.queue_announcements()
    reply = native.queue.get_nowait()
    native.queue.task_done()
    assert reply.speech_text == "实际结果是四十二。"
    await native.send_item(reply.items[0])
    context = native.upstream_send.call_args.args[0]
    assert [item["role"] for item in context["items"]] == ["user", "assistant"]
    reply.requested_at = 1
    native.replies[reply.request_id] = reply
    for event in native.protocol.normalize(
        {"type": "response.output_audio.started", "response_id": "spoken"}
    ):
        await native.provider_event(event)
    final = [c.args[0] for c in native.emit.call_args_list if c.args[0]["type"] == "transcript.final"]
    assert final[-1]["content"] == "实际结果是四十二。" and final[-1]["origin"] == "announcement"


def test_tool_output_json_string_is_unwrapped_like_reference():
    event = conversation_item({"type": "function_call_output", "call_id": "c", "output": '"hello"'})
    assert event["items"][0]["content"][0]["text"] == "hello"


def test_completed_local_and_wire_aliases_are_bounded_and_keep_recent_ids():
    protocol = SeeduplexProtocol()
    for index in range(300):
        protocol.normalize({"type": "response.function_call_arguments.done", "items": []})
        protocol.normalize({"type": "response.output_audio.done", "response_id": str(index)})
    assert len(protocol.finished) == 512
    assert "299" in protocol.finished and "0" not in protocol.finished


@pytest.mark.parametrize("result", [{"ok": True}, {"error": True, "user_message": "failed"}])
async def test_native_tool_receipt_precedes_done_and_no_second_reply(native, result):
    native.tools.execute = AsyncMock(return_value=result)
    for event in native.protocol.normalize(
        {
            "type": "response.function_call_arguments.done",
            "response_id": "r",
            "items": [{"call_id": "c", "name": "lookup", "arguments": "{}"}],
        }
    ):
        await native.provider_event(event)
    await asyncio.gather(*native.tool_tasks)
    assert not native.responses["r"]["done"]
    receipts = [c.args[0] for c in native.upstream_send.call_args_list]
    assert len(receipts) == 1 and receipts[0]["items"][0]["call_id"] == "c"
    assert json.loads(receipts[0]["items"][0]["content"][0]["text"]) == result
    assert native.queue.empty()
    await native.tool_followup("r")
    assert native.upstream_send.call_count == 1
    assert native.active == "r"


async def test_native_parallel_tools_return_one_complete_batch(native):
    slow = asyncio.Event()

    async def execute(name, *args):
        if name == "slow":
            await slow.wait()
        return {"name": name}

    native.tools.execute = execute
    for event in native.protocol.normalize(
        {
            "type": "response.function_call_arguments.done",
            "response_id": "r",
            "items": [{"name": name, "call_id": name, "arguments": "{}"} for name in ["fast", "slow"]],
        }
    ):
        await native.provider_event(event)
    await asyncio.sleep(0.02)
    assert native.upstream_send.call_count == 0
    slow.set()
    await asyncio.gather(*native.tool_tasks)
    assert native.upstream_send.call_count == 1
    assert [item["call_id"] for item in native.upstream_send.call_args.args[0]["items"]] == ["fast", "slow"]


@pytest.mark.parametrize("boundary", ["interrupt", "reconnect"])
async def test_stale_tool_completion_cannot_resume_new_turn(native, boundary):
    started, release = asyncio.Event(), asyncio.Event()

    async def execute(*args):
        started.set()
        await release.wait()
        return {"ok": True}

    native.tools.execute = execute
    for event in native.protocol.normalize(
        {
            "type": "response.function_call_arguments.done",
            "response_id": "old",
            "items": [{"call_id": "c", "name": "lookup", "arguments": "{}"}],
        }
    ):
        await native.provider_event(event)
    await started.wait()
    if boundary == "interrupt":
        await native.interrupt()
    else:
        native.provider_epoch += 1
    release.set()
    await asyncio.gather(*native.tool_tasks)
    assert not any(
        c.args[0]["type"] == "conversation.item.create" for c in native.upstream_send.call_args_list
    )


async def test_auth_error_stops_reconnect_until_new_user_attempt(native):
    native.upstream = SimpleNamespace(close=AsyncMock())
    native.provider_requested.set()
    await native.provider_event({"type": "error", "error": {"message": "Invalid X-Api-Key"}})
    assert not native.provider_requested.is_set()
    assert native.upstream.close.called
    native.upstream = None


async def test_no_active_cancel_ack_releases_protocol_identity(native):
    for event in native.protocol.normalize(
        {"type": "response.output_text.delta", "response_id": "old", "text": "old"}
    ):
        await native.provider_event(event)
    await native.interrupt()
    native.cancelling = True
    await native.provider_event({"type": "error", "error": {"message": "no response to cancel"}})
    assert native.protocol.active is None
    for event in native.protocol.normalize(
        {"type": "response.output_text.delta", "response_id": "new", "text": "new"}
    ):
        await native.provider_event(event)
    assert native.active == "new" and not native.responses["new"]["suppressed"]


async def test_native_tool_continuation_updates_final_text_in_same_response(native):
    for text in ["我来查询。", "查询结果是晴天。"]:
        for raw in [
            {"type": "response.output_text.delta", "response_id": "r", "text": text},
            {"type": "response.output_text.done", "response_id": "r", "text": text},
        ]:
            for event in native.protocol.normalize(raw):
                await native.provider_event(event)
    assert native.responses["r"]["text"] == "查询结果是晴天。"
    assert native.store.history(native.id)[-1]["content"] == "查询结果是晴天。"
    finals = [
        call.args[0] for call in native.emit.call_args_list if call.args[0]["type"] == "transcript.final"
    ]
    assert [e["content"] for e in finals] == ["我来查询。", "查询结果是晴天。"]


async def test_interrupt_between_tool_receipts_drops_remaining_old_results(native):
    native.responses["r"] = {
        "provider_epoch": native.provider_epoch,
        "generation": native.generation,
        "suppressed": False,
        "calls": {"a": {"ok": True}, "b": {"ok": True}},
    }
    sent = []

    async def send(item):
        sent.append(item)
        native.generation += 1

    native.send_item = send
    await native.tool_followup("r")
    assert [e["call_id"] for e in sent] == ["a"]


async def test_busy_response_retry_preserves_interactive_text(native):
    reply = Reply(
        native.generation,
        items=[
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "new question"}]}
        ],
    )
    native.replies[reply.request_id] = reply
    await native.provider_event({"type": "error", "error": {"message": "Another response is in progress"}})
    retry = native.queue.get_nowait()
    assert retry.response["instructions"] == "new question"
    assert retry.items == []
    assert reply.done.is_set()


async def test_health_exposes_seeduplex_capabilities_without_credentials(running):
    _, _, http, port = running
    health = (await http.get("/api/health")).json()
    assert health["realtimeModel"] == "1.2.6.1"
    assert health["voiceConfigured"]
    assert not health["realtimeModelProfile"]["transportCapabilities"]["imageBufferInput"]
    assert "fixture-key" not in json.dumps(health)
    client = await open_client(port)
    try:
        assert "input.image_buffer" not in client.events[0].get("capabilities", [])
        await client.send("input_image_buffer.append", image="AAAA")
        await client.wait("error")
    finally:
        await client.ws.close()


async def test_released_slot_waits_for_reconnected_provider(native):
    native.idle.clear()
    waiting = asyncio.Event()
    wait_idle = native.idle.wait

    async def observed_wait():
        waiting.set()
        await wait_idle()

    native.idle.wait = observed_wait
    reply = Reply(
        native.generation,
        items=[
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "after reconnect"}],
            }
        ],
    )
    pump = asyncio.create_task(native.response_pump())
    try:
        await native.enqueue(reply)
        await asyncio.wait_for(waiting.wait(), 1)
        native.ready.clear()
        native.idle.set()
        await asyncio.sleep(0.02)
        assert not native.upstream_send.called and not reply.done.is_set()
        native.ready.set()
        async with asyncio.timeout(1):
            while not native.upstream_send.called:
                await asyncio.sleep(0)
        assert native.upstream_send.call_args.args[0]["text"] == "after reconnect"
    finally:
        pump.cancel()
        await asyncio.gather(pump, return_exceptions=True)


@pytest.mark.parametrize("input_enabled", [True, False])
@pytest.mark.parametrize("event", [
    {"type": "session.closed"},
    {"type": "error", "request_event_id": "lost-context",
     "error": {"message": "no session active, please create a session first"}},
])
async def test_lost_session_reconnects_without_waiting_for_socket_close(running, event, input_enabled):
    gateway, provider, _, port = running
    client = await open_client(port, input_enabled=input_enabled)
    try:
        session = gateway.active
        session.context_request_ids.append("lost-context")
        previous = provider.connections[-1]
        before = len(provider.received)
        # The provider ends the session but leaves the WebSocket open.
        await previous.send(json.dumps(event))
        await client.wait("voice.ready", timeout=3)
        assert len(provider.connections) == 2
        assert gateway.active is session and session.ready.is_set()
        resumed = provider.received[before:]
        assert resumed[0]["type"] == "session.create"
        control = "input_audio_unmute.commit" if input_enabled else "input_audio_mute.commit"
        assert any(e["type"] == control for e in resumed)
        await client.send("input.unmute")
        await client.speak(provider, "after reconnect")
        transcript = await client.wait("transcript.final", lambda e: e["role"] == "user")
        assert transcript["content"] == "after reconnect"
        await client.wait("audio.done")
    finally:
        await client.ws.close()


async def test_native_handshake_text_commit_and_mute_controls(running):
    _, provider, _, port = running
    client = await open_client(port, input_enabled=False)
    try:
        assert provider.received[0]["type"] == "session.create"
        assert provider.connections[0].request.headers["X-Api-Key"] == "fixture-key"
        assert any(e["type"] == "input_audio_mute.commit" for e in provider.received)
        await client.send("input.message", text="single commit")
        await client.wait("audio.done")
        assert len([e for e in provider.received if e["type"] == "speech_text_buffer.commit"]) == 1
        commit = next(e for e in provider.received if e["type"] == "speech_text_buffer.commit")
        assert commit["text"] == "Being result: single commit"
        await client.send("input.unmute")
        await asyncio.sleep(0.05)
        assert provider.received[-1]["type"] == "input_audio_unmute.commit"
        await client.send("input.mute")
        await asyncio.sleep(0.05)
        assert provider.received[-1]["type"] == "input_audio_mute.commit"
    finally:
        await client.ws.close()
    async with asyncio.timeout(2):
        while not any(e["type"] == "session.close" for e in provider.received):
            await asyncio.sleep(0.01)


async def test_refresh_during_graceful_provider_close_can_claim_new_session(running):
    gateway, _, _, port = running
    first = await open_client(port, instance="first")
    old = gateway.active
    entered, release = asyncio.Event(), asyncio.Event()
    close = old.close_provider

    async def delayed_close():
        entered.set()
        await release.wait()
        await close()

    old.close_provider = delayed_close
    second = None
    try:
        await first.ws.close()
        await asyncio.wait_for(entered.wait(), 2)
        assert old.closed
        second = await open_client(port, instance="reloaded")
        assert gateway.active is not old
        release.set()
        await asyncio.sleep(0.05)
        assert gateway.active and not gateway.active.closed
    finally:
        release.set()
        if second:
            await second.ws.close()


async def test_being_delegation_uses_native_tool_continuation(running):
    _, provider, _, port = running
    client = await open_client(port)
    try:
        await client.speak(provider, "delegate")
        await client.wait("tool.call", lambda e: e["status"] == "completed")
        await client.wait("audio.done")
        assert any(
            e["type"] == "conversation.item.create" and e["items"][0].get("role") == "tool"
            for e in provider.received
        )
        commits = [e for e in provider.received if e["type"] == "speech_text_buffer.commit"]
        assert not commits  # Native tool continuation needs no text-to-speech request.
    finally:
        await client.ws.close()
