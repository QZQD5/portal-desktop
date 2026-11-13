import asyncio
import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from conftest import open_client

from gateway.app import Gateway
from gateway.being import MEMORY_WRITE_KIND, MEMORY_WRITE_TOOL
from conftest import unit_settings as Settings
from conftest import image_artifacts
from gateway.conversation_context import snapshot
from gateway.store import now_ms
from gateway.tasks import TaskManager
from gateway.voice import VoiceSession
from test_voice_requests import call, model_tool, speech


def context_from(text):
    return json.loads(
        text.split("<conversation_context>\n", 1)[1]
        .split("\n", 1)[1]
        .split("\n</conversation_context>", 1)[0]
    )


@pytest.fixture
async def voice(tmp_path):
    gateway = Gateway(Settings(data_dir=tmp_path))
    session = VoiceSession(gateway, None, "context-test", {"capabilities": ["input.audio"]})
    session.upstream = SimpleNamespace(close=AsyncMock())
    session.upstream_send = AsyncMock()
    session.emit = AsyncMock()
    session.ready.set()
    yield session
    session.upstream = None
    await session.close()
    await gateway.close()


def response(voice):
    voice.responses["r"] = {
        "text": "old answer",
        "turn_id": voice.turn,
        "generation": voice.generation,
        "origin": "model",
        "final": True,
        "done": True,
    }
    return voice.responses["r"]


@pytest.mark.parametrize("terminal", [None, "interrupted", "completed", "skipped"])
async def test_terminal_playback_does_not_resurrect_evicted_messages(voice, terminal):
    info = response(voice)
    await voice.playback_started("r")
    voice.record_playback("r", terminal)
    assert info["playback_ended"]
    for index in range(105):
        voice.store.message(voice.id, "user", str(index), id=f"user-{index}")
    before = voice.store.history(voice.id, 100)
    voice.record_playback("r")
    await voice.playback_started("r")
    await voice.playback_ended("r")
    assert voice.store.history(voice.id, 100) == before


async def test_late_status_or_transcript_cannot_reinsert_evicted_active_message(voice):
    response(voice)
    await voice.playback_started("r")
    for index in range(105):
        voice.store.message(voice.id, "user", str(index), id=f"user-{index}")
    before = voice.store.history(voice.id, 100)
    await voice.playback_ended("r")
    await voice.transcript("assistant", "late final", final=True, response_id="r")
    assert voice.store.history(voice.id, 100) == before


@pytest.mark.parametrize("reason", ["output_muted", "audio_unavailable", "playback_overflow"])
async def test_skipped_output_is_not_playback_but_preserves_text(running, reason):
    gateway, _, _, port = running
    client = await open_client(port)
    client.auto_playback = False
    try:
        await client.send("input.message", text="quiet result")
        audio = await client.wait("audio.done")
        await client.send("playback.cancelled", responseId=audio["responseId"], reason=reason)
        await client.wait("task.notification.delivered")
        voice = gateway.active
        info = voice.responses[audio["responseId"]]
        assert not info.get("playback_started")
        assert info["playback_status"] == "skipped"
        assert not info.get("suppressed")
        await voice.playback_started(audio["responseId"])
        await voice.playback_ended(audio["responseId"])
        message = next(m for m in voice.store.history(voice.id) if m["id"] == "voice:" + audio["responseId"])
        assert message["playbackStatus"] == "skipped"
        assert message["content"] == "Being result: quiet result"
    finally:
        await client.ws.close()


async def test_skipped_stream_flushes_and_keeps_later_audio_transcripts(voice):
    info = response(voice)
    info.update(text="", final=False, done=False)
    await voice.transcript("assistant", "first", response_id="r", buffer_audio=True)
    await voice.playback_ended("r", cancelled=True, reason="output_muted")
    assert not info.get("pending_transcripts")
    await voice.transcript("assistant", "whole answer", response_id="r", final=True, buffer_audio=True)
    assert voice.store.history(voice.id)[-1]["playbackStatus"] == "skipped"


@pytest.mark.parametrize("skip_after_done", [False, True])
async def test_muted_native_reply_returns_to_idle_after_generation_finishes(voice, skip_after_done):
    await voice.provider_event({"type": "response.created", "response": {"id": "r"}})
    await voice.provider_event({"type": "response.output_text.delta", "response_id": "r", "delta": "answer"})
    await voice.provider_event({"type": "response.audio.delta", "response_id": "r", "delta": "AAAAAA=="})
    if not skip_after_done:
        await voice.playback_ended("r", cancelled=True, reason="output_muted")
    await voice.provider_event(
        {"type": "response.done", "response_id": "r", "response": {"status": "completed"}}
    )
    if skip_after_done:
        await voice.playback_ended("r", cancelled=True, reason="output_muted")
    states = [
        call.args[0]["state"] for call in voice.emit.await_args_list if call.args[0]["type"] == "voice.state"
    ]
    assert states[-1] == "idle" and not voice.playing and voice.active is None
    assert voice.store.history(voice.id)[-1]["playbackStatus"] == "skipped"


async def test_being_knowledge_survives_prompt_override_and_session_refresh(voice, tmp_path):
    (tmp_path / "PROMPT.md").write_text("Custom voice instructions", encoding="utf-8")
    voice.settings.frontend_prompt_dir = tmp_path
    await voice.send_session("session.create")
    voice.working_directory = "D:/new-workspace"
    await voice.send_session("session.update")

    events = [call.args[0] for call in voice.upstream_send.await_args_list]
    assert [event["type"] for event in events] == ["session.create", "session.update"]
    for event in events:
        instructions = event["session"]["instructions"]
        assert instructions.startswith("Custom voice instructions\n")
        assert "Town（Beings Town、小镇）" in instructions
        assert "篝火（Bonfire）" in instructions
        assert "围炉（Fireside）" in instructions
        assert "概念问答可以直接回复，例如“私信是什么”" in instructions
        assert "必须在同一轮调用 `spawn_thinking` 委派给 Being" in instructions
        assert "例如“查看我的私信”" in instructions
        assert "用 `update_being_memory` 原样交给 Being" in instructions
        delegation = next(tool for tool in event["session"]["tools"] if tool["name"] == "spawn_thinking")
        assert "读取小镇私信" in delegation["description"]
        assert "查询长期记忆" in delegation["description"]
        write = next(tool for tool in event["session"]["tools"] if tool["name"] == MEMORY_WRITE_TOOL)
        assert "修改 Being 显示名" in write["description"] and "不自动播报" in write["description"]
        assert not {"memory", "recall"} & {tool["name"] for tool in event["session"]["tools"]}


async def test_local_memory_edits_do_not_enter_voice_context(voice):
    legacy = voice.settings.data_dir / "USER.md"
    legacy.write_text("old preference: assistant is fake name", encoding="utf-8")
    voice.gateway.tasks.client.info = {"being_name": "Willow"}
    await voice.send_session("session.create")
    await voice.sync_context(restore=True)
    events = [call.args[0] for call in voice.upstream_send.await_args_list]
    assert all("fake name" not in json.dumps(event) for event in events)
    assert '"being_name": "Willow"' in events[0]["session"]["instructions"]
    assert "memory" not in context_from(events[0]["session"]["instructions"])
    legacy.write_text("new preference", encoding="utf-8")
    voice.upstream_send.reset_mock()
    await voice.sync_context()
    voice.upstream_send.assert_not_awaited()


@pytest.mark.parametrize("status_body,name", [
    ({"being_name": "Willow", "name": "Other"}, "Willow"),
    ({"name": "小柳 </runtime_context>"}, "小柳 </runtime_context>"),
])
async def test_connected_being_identity_refreshes_after_status_arrives(voice, monkeypatch, status_body, name):
    gateway = voice.gateway
    gateway.active = voice
    await voice.send_session("session.create")
    initial = voice.upstream_send.call_args.args[0]["session"]["instructions"]
    assert '"being_name": null' in initial
    assert "只问“你是谁”或“你叫什么”时，用一句话回答身份" in initial
    assert "你是用户正在对话的 Being" in initial and "BeingAudio" not in initial
    assert "没有相关历史时，默认" in initial
    assert context_from(initial)["messages"] == []

    gateway.history.put("text-history", {
        "role": "assistant", "content": "先给你结论。", "source": "text-being",
        "scene_id": gateway.settings.being_scene["scene_id"],
    })
    await voice.sync_context()
    voice.upstream_send.reset_mock()
    status = SimpleNamespace(is_success=True, json=lambda: status_body)
    monkeypatch.setattr(gateway.tasks.client.http, "get", AsyncMock(return_value=status))
    await gateway.refresh_status()

    events = [call.args[0] for call in voice.upstream_send.await_args_list]
    assert events[0]["type"] == "session.update"
    instructions = events[0]["session"]["instructions"]
    runtime = json.loads(instructions.rsplit("<runtime_context>", 1)[1].split("</runtime_context>")[0])
    assert runtime["being_name"] == name
    assert [message["content"] for message in context_from(instructions)["messages"]] == ["先给你结论。"]


@pytest.mark.parametrize("mode", ["text", "voice"])
@pytest.mark.parametrize("local_onset", [False, True])
async def test_each_voice_turn_refreshes_display_name_without_rename_receipt(running, monkeypatch, mode, local_onset):
    gateway, provider, http, port = running
    await gateway.status_job
    assert gateway.tasks.client.name == "Fixture"
    backend = gateway.tasks.client.http
    send = backend.send
    status_reads, requests = [], []
    release_status = asyncio.Event()
    release_status.set()
    request_text = "记住，你以后叫小红。"
    reply = "改好了，现在叫小红。"

    async def respond(request, **kwargs):
        if request.url.path.endswith("/api/chat/stream"):
            requests.append(json.loads(request.content)["message"])
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=(
                'event: meta\ndata: {"stream_id":"rename"}\n\n'
                'event: content_block_delta\ndata: ' + json.dumps({"delta": {"text": reply}}, ensure_ascii=False)
                + '\n\nevent: message_stop\ndata: {}\n\n'
            ))
        if request.url.path.endswith("/api/status"):
            name = "小红" if requests else "Fixture"
            status_reads.append(name)
            await release_status.wait()
            return httpx.Response(200, json={"being_name": name})
        return await send(request, **kwargs)

    monkeypatch.setattr(backend, "send", respond)
    model_tool(monkeypatch, provider, {request_text: (MEMORY_WRITE_TOOL, {"objective": "模型改写"})})
    finish = provider.finish

    async def model_identity(ws, state, rid, text):
        if text == "Reply: 你是谁？":
            async with asyncio.timeout(2):
                while '"being_name": "小红"' not in state["instructions"]:
                    await asyncio.sleep(0.01)
            text = "我是小红。"
        await finish(ws, state, rid, text)

    monkeypatch.setattr(provider, "finish", model_identity)
    client = await open_client(port)
    try:
        session = gateway.active
        upstream = session.upstream
        monkeypatch.setattr(session.audio_activity, "append", lambda _: local_onset)
        assert not status_reads
        if mode == "text":
            response = await http.post("/api/heart/chat/stream", json={
                "message": request_text, "scene_id": "fixture-scene",
            })
            assert response.status_code == 200 and reply in response.text
            assert not status_reads
        else:
            await client.speak(provider, request_text)
            await client.wait("task.completed")
            assert status_reads == ["Fixture"]

        assert len(requests) == 1 and gateway.tasks.client.name == "Fixture"
        submitted = request_text if mode == "text" else "模型改写"
        assert requests[0].startswith(submitted + "\n\n以下是语音会话背景")

        start = len(provider.received)
        release_status.clear()
        await client.speak(provider, "你是谁？")
        user = await client.wait("transcript.final", lambda e: e["role"] == "user" and e["content"] == "你是谁？")
        assert gateway.tasks.client.name == "Fixture"
        assert any(event["type"] == "input_audio_buffer.append" for event in provider.received[start:])
        assert not any(event["type"] == "speech_text_buffer.commit" for event in provider.received[start:])
        release_status.set()
        answer = await client.wait("transcript.final", lambda e: e["role"] == "assistant" and e["turnId"] == user["turnId"])
        assert answer["content"] == "我是小红。"
        await client.wait("audio.done", lambda e: e["turnId"] == user["turnId"])
        assert {e["responseId"] for e in client.events if e["type"] == "audio.delta" and e["turnId"] == user["turnId"]} == {
            answer["responseId"],
        }
        events = provider.received[start:]
        updated = next(i for i, event in enumerate(events) if event["type"] == "session.update"
                       and '"being_name": "小红"' in event["session"]["instructions"])
        assert not any(event["type"] == "speech_text_buffer.commit" for event in events)
        assert events[updated]["session"]["instructions"]
        assert answer["origin"] == "model"
        assert status_reads == (["Fixture", "小红"] if mode == "voice" else ["小红"])
        assert gateway.tasks.client.name == "小红"
        assert gateway.active is session and session.upstream is upstream

        release_status.clear()
        await client.speak(provider, "你好！")
        await client.wait("transcript.final", lambda e: e["role"] == "assistant" and e["content"] == "Reply: 你好！")
        assert status_reads.count("小红") == 2 and len(requests) == 1
    finally:
        release_status.set()
        await client.ws.close()


@pytest.mark.parametrize("text,tool,write", [
    ("记住我喜欢简洁回答。", MEMORY_WRITE_TOOL, True),
    ("学习这套工作流程，以后照着做。", MEMORY_WRITE_TOOL, True),
    ("下次也按这个习惯回复。", MEMORY_WRITE_TOOL, True),
    ("以后请把回答说短一点。", MEMORY_WRITE_TOOL, True),
    ("你还记得我的长期偏好吗？", "spawn_thinking", False),
])
async def test_voice_long_term_requests_are_sent_to_being(running, text, tool, write):
    gateway, provider, _, port = running
    startup = gateway.status_job
    await startup
    client = await open_client(port)
    try:
        session = gateway.active
        await speech(session, "memory-input", text)
        await call(session, "memory-response", tool, {"objective": "模型改写"})
        task = (await client.wait("task.completed"))["task"]
        assert task["objective"] == "模型改写" and task["result"] == "Being result: 模型改写"
        assert (task["kind"] == MEMORY_WRITE_KIND) is write
        assert task["notificationStatus"] == ("none" if write else "pending")
        assert len(gateway.tasks.tasks) == 1 and gateway.status_job is startup
        assert not (gateway.settings.data_dir / "USER.md").exists()
        assert not (gateway.settings.data_dir / "MEMORY.md").exists()
    finally:
        await client.ws.close()


@pytest.mark.parametrize("failed", [False, True])
async def test_memory_write_acknowledges_before_being_returns_and_keeps_result_quiet(running, monkeypatch, failed):
    gateway, provider, _, port = running
    send = gateway.tasks.client.http.send
    finish = provider.finish
    release = asyncio.Event()
    submitted = asyncio.Event()
    text = "记住你叫小明。"

    class Write(httpx.AsyncByteStream):
        async def __aiter__(self):
            await release.wait()
            event = "error" if failed else "content_block_delta"
            data = {"message": "写入失败"} if failed else {"delta": {"text": "后台已保存"}}
            yield f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()

    async def respond(request, **kwargs):
        if request.method == "POST" and request.url.path.endswith("/api/chat/stream"):
            assert json.loads(request.content)["message"].startswith("模型改写\n\n")
            submitted.set()
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Write())
        return await send(request, **kwargs)

    async def acknowledge(ws, state, rid, response):
        if response in {"Reply: " + text, "tool completed"}:
            response = "好，我知道了。"
        await finish(ws, state, rid, response)

    monkeypatch.setattr(gateway.tasks.client.http, "send", respond)
    monkeypatch.setattr(provider, "finish", acknowledge)
    model_tool(monkeypatch, provider, {text: (MEMORY_WRITE_TOOL, {"objective": "模型改写"})})
    client = await open_client(port)
    try:
        await client.speak(provider, text)
        ack = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert ack["content"] == "好，我知道了。"
        await client.wait("audio.done", lambda e: e["responseId"] == ack["responseId"])
        await asyncio.wait_for(submitted.wait(), 2)
        assert len(gateway.tasks.tasks) == 1
        task = next(iter(gateway.tasks.tasks.values()))
        assert task["kind"] == MEMORY_WRITE_KIND and task["status"] == "running"
        receipts = [json.loads(item["content"][0]["text"]) for event in provider.received
                    if event["type"] == "conversation.item.create"
                    for item in event["items"] if item.get("call_id") == "model-update_being_memory-call"]
        assert [r["status"] for r in receipts] == ["accepted"]
        assert "user_message" not in receipts[0]
        await gateway.tasks.report_progress(task, "正在保存设置。")
        assert not any(event["type"] == "speech_text_buffer.commit" for event in provider.received)
        release.set()
        terminal = (await client.wait("task.failed" if failed else "task.completed"))["task"]
        assert terminal["notificationStatus"] == "none"
        if failed:
            assert "写入失败" in terminal["error"]
        else:
            assert terminal["result"] == "后台已保存"
        await gateway.active.queue_announcements()
        assert not any(event["type"] == "speech_text_buffer.commit" for event in provider.received)
    finally:
        release.set()
        await client.ws.close()


async def test_status_refresh_keeps_name_on_timeout_and_error(voice, monkeypatch):
    gateway = voice.gateway
    client = gateway.tasks.client
    client.info = {"being_name": "Willow"}

    async def stalled_status(*args, **kwargs):
        await asyncio.Future()

    get = AsyncMock(side_effect=stalled_status)
    monkeypatch.setattr(client.http, "get", get)
    await gateway.refresh_status(timeout=.01)
    assert client.name == "Willow" and client.error == "TimeoutError"

    get.side_effect = lambda *args, **kwargs: httpx.Response(200, json={"being_name": "小柳"})
    await gateway.refresh_status()
    assert get.await_count == 2 and client.name == "小柳"

    get.side_effect = lambda *args, **kwargs: httpx.Response(503)
    await gateway.refresh_status()
    assert client.name == "小柳"


async def test_identity_question_does_not_make_gateway_speak(voice, monkeypatch):
    voice.gateway.tasks.client.info = {"being_name": "当前名字"}
    refresh = AsyncMock(return_value=None)
    monkeypatch.setattr(voice.gateway, "refresh_status", refresh)
    await speech(voice, "identity", "你叫什么名字呀？")
    await voice.speech_status_job
    refresh.assert_awaited_once()
    assert '"being_name": "当前名字"' in voice.instructions()
    assert voice.queue.empty()


async def test_desktop_name_is_used_on_provider_create_and_updated_before_audio(running):
    gateway, provider, _, port = running
    await gateway.status_job
    client = await open_client(port, client_type="desktop", being_name="小红")
    try:
        first = next(event for event in provider.received if event["type"] == "session.create")
        assert '"being_name": "小红"' in first["session"]["instructions"]
        for name, expected in [("Fv", "Fv"), ("", "Fixture")]:
            start = len(provider.received)
            await client.send("input_audio_buffer.append", audio="AAAAAA==", being_name=name)
            await client.wait("voice.ready")
            await client.wait("transcript.final", lambda event: event["role"] == "user")
            events = provider.received[start:]
            create = next(i for i, event in enumerate(events) if event["type"] == "session.create")
            audio = next(i for i, event in enumerate(events) if event["type"] == "input_audio_buffer.append")
            assert create < audio
            assert json.dumps({"being_name": expected}, ensure_ascii=False)[1:-1] in events[create]["session"]["instructions"]
    finally:
        await client.ws.close()


async def test_rejected_context_retries_are_bounded_and_new_content_can_sync(voice):
    for attempt in range(3):
        await voice.sync_context()
        request = voice.upstream_send.call_args.args[0]["event_id"]
        voice.active = "ongoing-voice"
        await voice.provider_event(
            {
                "type": "error",
                "error": {
                    "event_id": request,
                    "message": "Another response is in progress",
                },
            }
        )
        assert voice.context_sent is None
        assert voice.active == "ongoing-voice"
        assert voice.context_failures == attempt + 1
    count = voice.upstream_send.await_count
    await voice.sync_context()
    assert voice.upstream_send.await_count == count
    voice.store.message(voice.id, "user", "new data")
    await voice.sync_context()
    assert voice.upstream_send.await_count > count
    assert context_from(voice.upstream_send.call_args.args[0]["items"][0]["content"][0]["text"])["snapshot"]


async def test_rejection_while_sending_cannot_restore_a_failed_cursor(voice):
    async def reject(event):
        if event["type"] == "conversation.item.create":
            await voice.provider_event(
                {
                    "type": "error",
                    "error": {
                        "event_id": event["event_id"],
                        "message": "temporarily unavailable",
                    },
                }
            )

    voice.upstream_send = AsyncMock(side_effect=reject)
    await voice.sync_context()
    assert voice.context_sent is None and voice.instructions_config is None
    assert voice.context_failures == 1
    voice.upstream_send = AsyncMock()
    await voice.sync_context()
    assert voice.context_sent is not None


async def test_unattributed_provider_error_invalidates_context(voice):
    await voice.sync_context()
    await voice.provider_event({"type": "error", "error": {"message": "temporarily unavailable"}})
    assert voice.context_sent is None
    await voice.sync_context()
    assert voice.context_sent is not None


async def test_queued_request_keeps_its_original_context_and_discards_old_local_memory(voice):
    manager = voice.gateway.tasks
    voice.store.message(voice.id, "assistant", "A: red, green", turnId="a")
    voice.store.message(voice.id, "user", "use second", turnId="a")
    task = await manager.submit("use second", voice.id, "a")
    voice.store.message(voice.id, "user", "unrelated B", turnId="b")
    voice.store.message(voice.id, "assistant", "B: large, small", turnId="b")
    key = "task_context:" + task["id"]
    voice.store.put(key, {**voice.store.get(key), "memory": [{"scope": "memory", "content": "old local memory"}]})
    captured = []

    async def run(task, context, save, progress, output=None):
        captured.append(context_from(context))
        return "done"

    manager.client.run = run
    await manager.execute(task)
    assert [m["content"] for m in captured[0]["messages"]] == ["A: red, green", "use second"]
    assert captured[0]["currentRequest"]["taskId"] == task["id"]
    assert captured[0]["currentRequest"]["turnId"] == "a"
    assert "memory" not in captured[0] and "memory" not in voice.store.get(key)
    assert task["contextVersion"] == captured[0]["version"]
    assert "messages" not in task
    manager.settings.terminal_task_ttl = 0
    manager.settings.pending_notification_ttl = 0
    manager.prune()
    assert voice.store.get("task_context:" + task["id"]) is None


async def test_scheduled_context_survives_restart_and_recurrence(voice):
    from gateway.store import now_ms

    manager = voice.gateway.tasks
    voice.store.message(voice.id, "user", "original context", turnId="a")
    task = await manager.submit(
        "scheduled",
        voice.id,
        "a",
        schedule={
            "type": "task",
            "recurrence": "daily",
            "executeAt": now_ms() - 10,
        },
    )
    voice.store.message(voice.id, "user", "unrelated later chat", turnId="b")
    restored = TaskManager(manager.settings, voice.store, AsyncMock())
    await restored.tick_schedules()
    next_task = next(t for t in restored.tasks.values() if t["id"] != task["id"])
    context = voice.store.get("task_context:" + next_task["id"])
    assert [m["content"] for m in context["messages"]] == ["original context"]
    assert context["currentRequest"]["taskId"] == next_task["id"]


def large_attachment():
    return {
        "type": "file",
        "mime": "application/octet-stream",
        "filename": "sample.bin",
        "url": "data:application/octet-stream;base64," + base64.b64encode(bytes(8 * 1024 * 1024)).decode(),
    }


def assert_metadata_only(value):
    encoded = json.dumps(value)
    assert "base64," not in encoded
    assert len(encoded) < 64000


async def test_large_task_events_keep_original_input_without_exceeding_journal_budget(voice):
    manager = voice.gateway.tasks
    part = large_attachment()
    task = await manager.submit("inspect attachment", voice.id, parts=[part])
    accepted = voice.store.replay(voice.id)["events"]
    assert len(accepted) == 1 and accepted[0]["type"] == "task.accepted"
    assert_metadata_only(accepted)
    assert accepted[0]["task"]["parts"][0]["filename"] == part["filename"]
    assert_metadata_only(manager.list())
    task.update(status="completed", result="read successfully", completedAt=now_ms())
    await manager.publish(task)
    assert_metadata_only(voice.store.replay(voice.id)["events"])
    assert voice.store.get("tasks")[task["id"]]["parts"] == [part]


async def test_voice_context_bounds_task_summaries_and_large_attachments(voice):
    manager = voice.gateway.tasks
    part = large_attachment()
    for index in range(2):
        task = await manager.submit(str(index) + "x" * 31999, voice.id, parts=[part])
        task["activity"] = [{"text": "large diagnostic " * 10000}]
    instructions = voice.session_config(snapshot(voice.store, manager.tasks, voice.id))["instructions"]
    tasks = context_from(instructions)["tasks"]
    assert len(tasks) == 2
    assert all(len(task["objective"]) <= 2000 for task in tasks)
    assert all("parts" not in task and "activity" not in task for task in tasks)
    assert_metadata_only(instructions)

    class Upstream:
        send = AsyncMock()
        close = AsyncMock()

    voice.upstream = Upstream()
    voice.upstream_send = VoiceSession.upstream_send.__get__(voice)
    await voice.send_session("session.create")
    await asyncio.gather(*voice.upstream_send_tasks)
    voice.upstream.send.assert_awaited_once()
    voice.upstream.close.assert_not_awaited()


async def test_cross_session_text_result_is_available_to_followup_and_voice_restore(voice):
    manager = voice.gateway.tasks
    task = await manager.submit("make a plan", "original-session", "original-turn")
    task.update(status="completed", result="Option red and option blue", notificationStatus="pending")
    await manager.publish(task)
    await voice.text_announcements()
    shown = next(
        call.args[0] for call in voice.emit.await_args_list if call.args[0]["type"] == "transcript.final"
    )
    stored = voice.store.history(voice.id)
    assert len(stored) == 1 and stored[0]["id"] == shown["messageId"]
    assert stored[0]["content"] == shown["content"] == task["result"]
    assert stored[0]["taskId"] == task["id"]
    assert stored[0]["sourceSessionId"] == "original-session"
    assert (
        snapshot(voice.store, manager.tasks, voice.id)["messages"][0]["sourceSessionId"] == "original-session"
    )
    await voice.text_announcements()
    assert voice.store.history(voice.id) == stored
    followup = await manager.submit("use the second option", voice.id)
    assert voice.store.get("task_context:" + followup["id"])["messages"][0]["content"] == task["result"]
    await voice.sync_context(restore=True)
    restored = context_from(voice.upstream_send.call_args.args[0]["items"][0]["content"][0]["text"])
    assert restored["messages"][0]["content"] == task["result"]


async def test_task_queries_and_socket_snapshot_do_not_return_attachment_bodies(running):
    gateway, _, http, port = running
    task = await gateway.tasks.submit(
        "future attachment",
        "test-session",
        schedule={"type": "task", "executeAt": now_ms() + 100000},
        parts=[large_attachment()],
    )
    client = await open_client(port, text_only=True)
    try:
        assert_metadata_only(await client.wait("task.snapshot"))
        for path in ("/api/tasks", "/api/tasks/" + task["id"]):
            response = await http.get(path)
            assert response.status_code == 200
            assert_metadata_only(response.json())
        for kind, args in (("task.list", {}), ("task.get", {"task_id": task["id"]})):
            await client.send(kind, **args)
            assert_metadata_only(await client.wait(kind + ".result"))
        async with http.stream("GET", "/api/tasks/" + task["id"] + "/events") as stream:
            async for line in stream.aiter_lines():
                if line.startswith("data: "):
                    assert_metadata_only(json.loads(line[6:]))
                    break
        status = await gateway.tools.execute("get_agent_task_status", {"task_id": task["id"]}, gateway.active)
        assert_metadata_only(status)
        await client.send("task.cancel", task_id=task["id"])
        assert_metadata_only(await client.wait("task.cancel.result"))
        cancelled = await http.delete("/api/tasks/" + task["id"])
        assert cancelled.status_code == 409
        assert_metadata_only(cancelled.json())

        await client.send(
            "task.create",
            message={"parts": [{"type": "text", "text": "read attachment"}, *task["parts"]]},
        )
        created = await client.wait("task.create.result")
        assert_metadata_only(created)
        completed = await client.wait("task.completed")
        assert completed["task"]["id"] == created["task"]["id"]
        assert completed["task"]["result"] == "Being result: read attachment"

        future = await gateway.tasks.submit(
            "cancel via HTTP",
            "test-session",
            schedule={"type": "task", "executeAt": now_ms() + 100000},
            parts=task["parts"],
        )
        cancelled = await http.delete("/api/tasks/" + future["id"])
        assert cancelled.status_code == 200
        assert_metadata_only(cancelled.json())
    finally:
        await client.ws.close()


async def test_voice_task_tools_keep_attachment_bodies_private(voice):
    manager = voice.gateway.tasks
    voice.input_parts = [large_attachment()]
    created = await voice.tools.execute("spawn_thinking", {"objective": "read attachment"}, voice)
    assert_metadata_only(created)
    task = manager.tasks[created["task_id"]]
    assert task["parts"] == voice.input_parts
    task["submissionKey"] = f"typed:{voice.id}:{voice.turn}"
    duplicate = await voice.tools.execute("spawn_thinking", {"objective": "same typed input"}, voice)
    assert duplicate["status"] == "already_submitted"
    assert_metadata_only(duplicate)
    await speech(voice, "cancel", "取消刚才的任务")
    await call(voice, "cancel-reply", "cancel_agent_task", {"task_id": task["id"]})
    await asyncio.gather(*voice.tool_tasks, *manager.cancellations.values())
    cancelled = voice.responses["cancel-reply"]["calls"]["cancel-reply-call"]
    assert cancelled["status"] == "accepted" and cancelled["task_ids"] == [task["id"]]
    assert_metadata_only(cancelled)


@pytest.mark.parametrize("imported", [False, True])
async def test_old_task_journals_are_projected_without_changing_saved_inputs(voice, imported):
    task = await voice.gateway.tasks.submit(
        "old task",
        voice.id,
        parts=[
            {
                "type": "file",
                "mime": "text/plain",
                "filename": "old.txt",
                "url": "data:text/plain;base64,YQ==",
            }
        ],
    )
    # Simulate a journal written before task attachment projection existed.
    event = {"type": "task.accepted", "task": task}
    if imported:
        event = {
            "type": "journal.import",
            "record": {
                "type": "qwaudio/task/event",
                "taskId": task["id"],
                "payload": {"domainType": "task.accepted", "task": task},
            },
        }
    voice.store.event(voice.id, event)
    voice.store.event(voice.id, {"type": "task.snapshot", "tasks": [task]})
    assert_metadata_only(voice.store.replay(voice.id))
    assert_metadata_only(voice.store.journal(voice.id))
    assert_metadata_only(voice.store.session_replay(voice.id))
    assert voice.store.get("tasks")[task["id"]]["parts"][0]["url"].startswith("data:")




@pytest.mark.parametrize("outputs", ["text", "artifacts"])
async def test_task_snapshot_connects_after_reload_with_large_history(tmp_path, outputs):
    settings = Settings(data_dir=tmp_path)
    gateway = Gateway(settings)
    count = 30 if outputs == "text" else 2
    try:
        for index in range(count):
            task = await gateway.tasks.submit(str(index), "old-session")
            task.update(
                status="completed", result="字" * 100000, completedAt=now_ms(), notificationStatus="delivered"
            )
            if outputs == "artifacts":
                task["artifacts"] = image_artifacts(
                    [
                        {
                            "type": "image",
                            "mimeType": "image/png",
                            "data": base64.b64encode(bytes(4 * 1024 * 1024)).decode(),
                        }
                    ]
                )
            await gateway.tasks.publish(task)
    finally:
        await gateway.close()
    restored = Gateway(settings)
    socket = SimpleNamespace(
        query_params={"sessionId": "fresh-session"}, send_text=AsyncMock(), close=AsyncMock()
    )
    hello = {
        "event_id": "hello",
        "client": {"type": "web", "instance_id": "client"},
        "connection": {"text_only": True},
    }
    try:
        session = await restored.claim(socket, hello, "client", [])
        assert session.background
        await asyncio.gather(*session.client_send_tasks)
        sent = [json.loads(call.args[0]) for call in socket.send_text.await_args_list]
        task_snapshot = next(event for event in sent if event["type"] == "task.snapshot")
        assert len(task_snapshot["tasks"]) == count
        assert len(json.dumps(task_snapshot, ensure_ascii=False).encode()) < 4 * 1024 * 1024
        await restored.send(session, {"type": "task.list.result", "tasks": task_snapshot["tasks"]})
        assert restored.store.replay(session.id)["events"] == []
        assert all(task["result"] == "字" * 100000 for task in restored.tasks.tasks.values())
    finally:
        await restored.close()


async def test_full_task_http_keeps_deferred_output_and_hides_input_attachment(running):
    gateway, _, http, _ = running
    task = await gateway.tasks.submit(
        "large output",
        "http-details",
        parts=[large_attachment()],
        schedule={"type": "task", "executeAt": now_ms() + 100000},
    )
    raw = base64.b64encode(bytes(7 * 1024 * 1024)).decode()
    task.update(
        status="completed", result="字" * 100000, notificationStatus="delivered", completedAt=now_ms()
    )
    task["artifacts"] = image_artifacts([{"type": "image", "mimeType": "image/png", "data": raw}])
    await gateway.tasks.publish(task)
    listing = (await http.get("/api/tasks")).json()["tasks"]
    assert len(json.dumps(listing).encode()) < 32768
    detail = await http.get("/api/tasks/" + task["id"])
    assert detail.status_code == 200
    detail = detail.json()
    assert detail["result"] == task["result"]
    assert detail["artifacts"][0]["parts"][0]["raw"] == raw
    assert "url" not in detail["parts"][0]


async def test_replay_pages_bound_total_bytes_without_losing_snapshot_records(voice):
    tasks = [{"id": str(index), "result": "x" * 24000} for index in range(100)]
    for _ in range(10):
        voice.store.event(voice.id, {"type": "task.snapshot", "tasks": tasks})
    sequence, seen = 0, []
    while True:
        page = voice.store.replay(voice.id, sequence, 200)
        assert len(json.dumps(page).encode()) < 4 * 1024 * 1024
        assert page["next_sequence"] > sequence
        seen.extend(event["sequence"] for event in page["events"])
        sequence = page["next_sequence"]
        if not page["has_more"]:
            break
    assert seen == list(range(1, 11))
    assert voice.store.replay(voice.id, sequence)["events"] == []
