import asyncio
import base64
import json
import logging
import time
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from conftest import open_client

from gateway.app import Gateway
from conftest import unit_settings as Settings
from gateway.retention import public_task
from gateway.voice import Reply


@pytest.mark.parametrize("phase", ["unacknowledged", "first_audio", "mid_audio"])
async def test_speech_deadline_recovers_without_waiting_for_generic_response_timeout(running, monkeypatch, phase):
    gateway, provider, _, port = running
    gateway.settings.response_timeout = 60
    gateway.settings.speech_first_audio_timeout = .1
    gateway.settings.speech_audio_inactivity_timeout = .1
    original_finish, original_reply = provider.finish, provider.reply

    async def stop_audio(ws, state, rid, text):
        if phase == "mid_audio":
            await ws.send(json.dumps({"type": "response.output_audio.delta", "response_id": rid, "audio": "AAAAAA=="}))
        # Text activity must not keep a silent audio stream alive.
        while state["active"] == rid:
            await ws.send(json.dumps({"type": "response.output_text.delta", "response_id": rid, "text": "仍有文字"}))
            await asyncio.sleep(.03)

    monkeypatch.setattr(provider, "finish", stop_audio)
    if phase == "unacknowledged":
        monkeypatch.setattr(provider, "reply", AsyncMock())
    client = await open_client(port)
    try:
        session = gateway.active
        reply = Reply(session.generation, speech_text="已经查到的完整结果")
        await session.enqueue(reply)
        event = await client.wait("error", lambda e: e.get("error", {}).get("code") == "response_failed", timeout=4)
        assert event["error"]["reason"] == ("audio_inactivity_timeout" if phase == "mid_audio" else "first_audio_timeout")
        await asyncio.wait_for(reply.done.wait(), 3)
        assert reply.failed
        if phase == "unacknowledged":
            await client.wait("voice.ready")
        monkeypatch.setattr(provider, "finish", original_finish)
        monkeypatch.setattr(provider, "reply", original_reply)
        await session.enqueue(Reply(session.generation, speech_text="恢复后的播报"))
        await client.wait("transcript.final", lambda e: e.get("content") == "恢复后的播报")
        await client.wait("audio.done")
        assert not gateway.tasks.tasks
    finally:
        await client.ws.close()


@pytest.mark.parametrize("cloud_start", [False, True])
async def test_idle_missing_asr_reconnects_and_accepts_next_utterance(running, monkeypatch, cloud_start):
    gateway, provider, _, port = running
    original_voice = provider.voice_turn

    async def empty_asr(ws, state):
        if cloud_start and not state.get("empty_started"):
            state["empty_started"] = True
            await ws.send(json.dumps({"type": "conversation.item.input_audio_transcription.started", "item_id": "empty"}))

    monkeypatch.setattr(provider, "voice_turn", empty_asr)
    client = await open_client(port)
    try:
        session = gateway.active
        pcm = (Path(__file__).parent / "fixtures/speech-stop.pcm").read_bytes() + bytes(32000)
        await client.send("audio.append", audio=base64.b64encode(pcm).decode())
        record = None
        async with asyncio.timeout(3):
            while record is None or "started_at" not in record:
                record = session.input_records.get("empty") if cloud_start else session.local_barge_in
                await asyncio.sleep(.01)
        assert not record.get("interrupted_output")
        record["started_at"] -= 6
        await session.recover_missing_barge_in_transcription()
        record["endpoint_requested_at"] -= 2.1
        # The regular supervisor performs the recovery, without toggling voice.
        await client.wait("error", lambda e: e.get("error", {}).get("code") == "transcription_unavailable")
        await client.wait("voice.ready")
        monkeypatch.setattr(provider, "voice_turn", original_voice)
        await client.speak(provider, "重连后的问题")
        recognized = await client.wait("transcript.final", lambda e: e["role"] == "user")
        assert recognized["content"] == "重连后的问题"
        await client.wait("audio.delta")
        assert len(provider.connections) >= 2 and not gateway.tasks.tasks
    finally:
        await client.ws.close()


async def test_stalled_progress_query_reconnects_and_speaks_without_waiting_for_backend(running, caplog):
    caplog.set_level(logging.INFO, logger="gateway.voice")
    gateway, provider, _, port = running
    client = await open_client(port)
    try:
        session = gateway.active
        for raw in [
            {"type": "conversation.item.input_audio_transcription.started", "item_id": "stalled"},
            {"type": "conversation.item.input_audio_transcription.delta", "item_id": "stalled", "delta": "到哪一步了"},
        ]:
            await provider.connections[-1].send(json.dumps(raw))
        await client.wait("transcript.delta", lambda e: e["role"] == "user")
        record = session.input_records["stalled"]
        record["text_updated"] = time.monotonic() - 5.1
        record["audio_updated"] = time.monotonic()
        await session.recover_stalled_transcription()
        record["endpoint_requested_at"] -= 2.1
        user = await client.wait("transcript.final", lambda e: e["role"] == "user")
        assert user["content"] == "到哪一步了" and user["unconfirmed"]
        answer = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert "语音识别没有完成" in answer["content"]
        await client.wait("audio.delta")
        assert not gateway.tasks.tasks
        received = [r.getMessage() for r in caplog.records if "realtime.provider_received" in r.getMessage()]
        assert any("input_audio_transcription.started" in message and "item=stalled" in message for message in received)
        assert any("input_audio_transcription.delta" in message and "item=stalled" in message for message in received)
        assert all("到哪一步了" not in message and "fixture-key" not in message for message in received)
        assert "reason=transcription_timeout" in caplog.text
        kinds = [e["type"] for e in provider.received]
        assert kinds.index("input_audio_buffer.commit") < kinds.index("session.close")
    finally:
        await client.ws.close()


@pytest.mark.parametrize("text", ["好了", "停"])
async def test_stalled_barge_in_reconnects_without_speaking(running, text):
    gateway, provider, _, port = running
    client = await open_client(port)
    client.auto_playback = False
    try:
        session = gateway.active
        await session.enqueue(Reply(session.generation, speech_text="正在播报一段较长的内容。"))
        prior = await client.wait("audio.done")
        await client.send("playback.started", responseId=prior["responseId"])
        for raw in [
            {"type": "conversation.item.input_audio_transcription.started", "item_id": "stop"},
            {"type": "conversation.item.input_audio_transcription.delta", "item_id": "stop", "delta": text},
        ]:
            await provider.connections[-1].send(json.dumps(raw))
        await client.wait("transcript.delta", lambda e: e["role"] == "user")
        record = session.input_records["stop"]
        assert record["interrupted_output"]
        record["text_updated"] = time.monotonic() - 5.1
        record["audio_updated"] = time.monotonic()
        await session.recover_stalled_transcription()
        record["endpoint_requested_at"] -= 2.1
        user = await client.wait("transcript.final", lambda e: e["role"] == "user")
        assert user["content"] == text and user["unconfirmed"]
        await client.wait("voice.ready")
        commits = [e for e in provider.received if e["type"] == "speech_text_buffer.commit"]
        assert len(commits) == 1
        assert not any(
            e["type"] == "transcript.final" and e.get("role") == "assistant" and e.get("turnId") == record["turn"]
            for e in client.events
        )
        assert len(provider.connections) >= 2 and not gateway.tasks.tasks
        assert not any(e["type"] == "response.cancel" for e in provider.received)
    finally:
        await client.ws.close()


async def test_result_and_asr_both_silent_recover_without_toggling_voice(running, monkeypatch):
    gateway, provider, _, port = running
    client = await open_client(port)
    original_reply, original_voice = provider.reply, provider.voice_turn
    monkeypatch.setattr(provider, "reply", AsyncMock())
    monkeypatch.setattr(provider, "voice_turn", AsyncMock())
    try:
        session = gateway.active
        task = await gateway.tasks.submit("已完成的查找", session.id)
        task.update(status="completed", result="完整查找结果", notificationStatus="pending")
        await session.enqueue(Reply(session.generation, origin="announcement", speech_text=task["result"]))
        for _ in range(100):
            if any(e["type"] == "speech_text_buffer.commit" for e in provider.received):
                break
            await asyncio.sleep(.01)
        assert not session.active and not session.idle.is_set()
        await session.stop_playback_on_speech()
        await session.append_audio("AAAAAA==")
        assert session.local_barge_in
        session.local_barge_in["started_at"] -= 6
        session.audio_activity.quiet_frames = 30
        await session.recover_missing_barge_in_transcription()
        session.local_barge_in["endpoint_requested_at"] -= 2.1
        await session.recover_missing_barge_in_transcription()
        await client.wait("error", lambda e: e.get("error", {}).get("code") == "transcription_unavailable")
        await client.wait("voice.ready")
        assert len(provider.connections) >= 2
        assert task["status"] == "completed" and len(gateway.tasks.tasks) == 1
        monkeypatch.setattr(provider, "reply", original_reply)
        monkeypatch.setattr(provider, "voice_turn", original_voice)
        await client.speak(provider, "恢复后的新问题")
        recognized = await client.wait("transcript.final", lambda e: e["role"] == "user")
        assert recognized["content"] == "恢复后的新问题"
        await client.wait("audio.delta")
    finally:
        await client.ws.close()


async def test_silent_backend_stays_quiet_and_status_is_local(tmp_path):
    gateway = Gateway(Settings(data_dir=tmp_path, backend_enabled=False))
    manager = gateway.tasks
    task = await manager.submit("list desktop", "s")
    task.update(status="running", startedAt=int(time.time() * 1000) - 1000)
    events = []
    manager.emit = AsyncMock(side_effect=lambda _, event: events.append(event))
    release = asyncio.Event()

    async def request(*args, **kwargs):
        await release.wait()
        return "done"

    manager.client.run = request
    pending = asyncio.create_task(manager.execute(task))
    try:
        await asyncio.sleep(1.3)
        assert not pending.done()
        assert [e["type"] for e in events] == ["task.running"]
        assert public_task(task)["elapsedMs"] >= 1000
        assert task["status"] == "running"
    finally:
        release.set()
        await pending
        await gateway.close()


@pytest.mark.parametrize("lead_in", [False, True])
async def test_stalled_delegation_releases_voice_for_the_being_result(running, monkeypatch, lead_in):
    gateway, provider, _, port = running
    release = asyncio.Event()

    async def blocked(*args, **kwargs):
        await release.wait()
        return "done"

    monkeypatch.setattr(gateway.tasks.client, "run", blocked)
    provider.silent_tool_reply = True
    provider.voice_lead_in = lead_in
    client = await open_client(port)
    try:
        await client.speak(provider, "delegate desktop")
        await client.wait("tool.call", lambda e: e["status"] == "completed")
        release.set()
        final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert final["content"] == "done"
        await client.wait("audio.delta", lambda e: e["responseId"] == final["responseId"])
        assert next(iter(gateway.tasks.tasks.values()))["status"] == "completed"
    finally:
        await client.ws.close()
        release.set()


async def test_native_lead_in_audio_does_not_wait_for_tool(running, monkeypatch):
    gateway, provider, _, port = running
    provider.voice_lead_in = True
    release = asyncio.Event()
    execute = gateway.tools.execute

    async def blocked(*args):
        await release.wait()
        return await execute(*args)

    monkeypatch.setattr(gateway.tools, "execute", blocked)
    client = await open_client(port)
    try:
        await client.speak(provider, "delegate desktop")
        await client.wait("audio.delta")
        assert not gateway.tasks.tasks
    finally:
        release.set()
        await client.ws.close()


async def test_pausing_stale_speech_releases_pending_task_result(running):
    gateway, _, _, port = running
    client = await open_client(port)
    try:
        session = gateway.active
        await session.provider_event(
            {"type": "input_audio_buffer.speech_started", "native_asr": True, "item_id": "unfinished"}
        )
        assert session.speaking_user
        session.native_pending_since = time.monotonic() - gateway.settings.response_timeout - 1
        task = await gateway.tasks.submit(
            "completed work",
            "test-session",
            schedule={"type": "reminder", "executeAt": 9999999999999},
        )
        task.update(
            status="completed", kind="work", result="已经读取桌面文件。", notificationStatus="pending"
        )
        await gateway.tasks.publish(task)
        await client.send("input.mute")
        final = await client.wait("transcript.final", lambda e: e.get("origin") == "announcement")
        assert final["content"] == task["result"]
        assert not session.speaking_user and not session.input_enabled
        assert session.output_enabled
        await client.wait("audio.delta")
    finally:
        await client.ws.close()


@pytest.mark.parametrize("disconnect", [False, True])
async def test_task_result_is_not_delivered_until_playback_finishes(running, disconnect):
    gateway, _, _, port = running
    client = await open_client(port)
    client.auto_playback = False
    try:
        await client.send("task.create", message={"parts": [{"type": "text", "text": "result"}]})
        task = (await client.wait("task.completed"))["task"]
        response = (await client.wait("audio.done"))["responseId"]
        await client.send("playback.started", responseId=response)
        await asyncio.sleep(0.05)
        assert gateway.tasks.tasks[task["id"]]["notificationStatus"] == "pending"
        assert gateway.active.responses[response]["turn_id"].startswith("announcement_")
        if disconnect:
            await client.ws.close()
            client = await open_client(port)
            client.auto_playback = False
            assert gateway.tasks.tasks[task["id"]]["notificationStatus"] == "none"
            assert gateway.history.find("task-result:" + task["id"])["content"] == task["result"]
            await client.send("task.create", message={"parts": [{"type": "text", "text": "new result"}]})
            task = (await client.wait("task.completed", lambda e: e["task"]["objective"] == "new result"))["task"]
            final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
            assert final["taskIds"] == [task["id"]]
            response = (await client.wait("audio.done", lambda e: e["responseId"] == final["responseId"]))["responseId"]
        await client.send("playback.ended", responseId=response)
        await client.wait("task.notification.delivered")
        assert gateway.tasks.tasks[task["id"]]["notificationStatus"] == "delivered"
    finally:
        await client.ws.close()


@pytest.mark.parametrize("same_socket", [False, True])
async def test_opening_voice_skips_already_completed_text_results(running, same_socket):
    gateway, _, _, port = running
    if same_socket:
        client = await open_client(port, text_only=True, output_enabled=False)
    old_task = await gateway.tasks.submit(
        "旧文字任务", "text", chat_stream_id="old-stream",
        schedule={"type": "task", "executeAt": 9_999_999_999_999},
    )
    old_task.update(status="completed", result="旧的文字结果", notificationStatus="pending")
    await gateway.tasks.publish(old_task)
    if same_socket:
        await client.send("unmute")
        await client.wait("voice.ready")
    else:
        client = await open_client(port)
    try:
        assert old_task["notificationStatus"] == "none"
        assert gateway.history.find("task-result:" + old_task["id"])["content"] == "旧的文字结果"
        await client.send("task.create", message={"parts": [{"type": "text", "text": "new result"}]})
        task = (await client.wait("task.completed", lambda e: e["task"]["objective"] == "new result"))["task"]
        final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert final["taskIds"] == [task["id"]]
        await client.wait("audio.done", lambda e: e["responseId"] == final["responseId"])
    finally:
        await client.ws.close()


async def test_correction_cancels_old_objective_and_executes_complete_new_objective(running, monkeypatch):
    gateway, _, _, port = running
    objectives = []

    async def prompt(task, *_):
        objectives.append(task["objective"])
        if task["objective"] == "分析物理内存使用情况":
            while task["status"] != "cancelling":
                await asyncio.sleep(0.01)
            await asyncio.Future()
        return "GPU 显存分析完成"

    monkeypatch.setattr(gateway.tasks.client, "run", prompt)
    client = await open_client(port)
    try:
        old = await gateway.tasks.submit("分析物理内存使用情况", "test-session")
        await client.wait("task.running")
        async with asyncio.timeout(3):
            while not objectives:
                await asyncio.sleep(0.01)
        session = gateway.active
        from test_voice_requests import call, speech

        await speech(session, "cancel", "取消刚才的任务，改查 GPU")
        await call(session, "cancel-reply", "cancel_agent_task", {"task_id": old["id"]})
        await asyncio.gather(*session.tool_tasks)
        receipt = await gateway.tools.execute(
            "spawn_thinking", {"objective": "修正原内存分析，改为分析 GPU 显存使用情况"}, session
        )
        new = await client.wait("task.completed", lambda e: e["task"]["id"] == receipt["task_id"])
        assert old["status"] == "cancelled" and old["notificationStatus"] == "none"
        assert new["task"]["result"] == "GPU 显存分析完成"
        assert objectives == ["分析物理内存使用情况", "修正原内存分析，改为分析 GPU 显存使用情况"]
        context = gateway.store.get("task_context:" + receipt["task_id"])
        assert context["messages"][-1]["content"] == "取消刚才的任务，改查 GPU"
    finally:
        await client.ws.close()
