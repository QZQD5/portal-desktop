import asyncio
import json

import httpx
import pytest

from conftest import open_client
from gateway.being import BeingClient
from gateway.store import Store, now_ms
from gateway.tasks import TaskManager
from gateway.task_activity import update_activity
from test_being import settings, event


async def test_voice_task_streams_activity_while_tool_waits_and_keeps_it_after_completion(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    class Work(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield event("meta", {"stream_id": "research", "scene_id": "desktop-scene"})
            yield event("thinking", {"text": "正在确定资料范围"})
            yield event("tool_use", {"tool_call_id": "search", "name": "web_search", "input": {"query": "文献", "token": "private-token"}})
            entered.set()
            await release.wait()
            yield event("tool_result", {"tool_call_id": "search", "content": "找到两份资料"})
            yield event("meta", {"scene_id": "another-scene", "continuation": True})
            yield event("tool_use", {"id": "unrelated", "name": "private_tool"})
            yield event("meta", {"scene_id": "desktop-scene", "continuation": True})
            yield event("content_block_delta", {"delta": {"text": "实际查到的资料"}})
            yield event("message_stop", {})
    async def respond(request):
        return httpx.Response(204) if request.method == "GET" else httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=Work())
    observed = []
    async def emit(_, item): observed.append(item)
    store = Store(tmp_path / "state.sqlite3")
    manager = TaskManager(settings(tmp_path), store, emit)
    await manager.client.close()
    manager.client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond), store=store)
    task = await manager.submit("查找资料", "voice-session")
    job = asyncio.create_task(manager.execute(task))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert task["status"] == "running" and not job.done()
        assert [entry["type"] for entry in task["activity"]] == ["think", "tool"]
        assert not task["activity"][-1].get("done")
        assert "private-token" not in task["activity"][-1]["inputDetail"]
        assert any(e["type"] == "task.activity" and e["task"]["activity"][-1].get("name") == "web_search"
                   for e in observed)
        release.set()
        await job
        assert task["status"] == "completed" and task["result"] == "实际查到的资料"
        assert task["activity"][-1]["done"] and task["activity"][-1]["result"] == "找到两份资料"
        assert all(e.get("name") != "private_tool" for e in task["activity"])
        assert store.get("tasks")[task["id"]]["activity"] == task["activity"]
    finally:
        release.set()
        await job
        await manager.close()
        store.close()


def test_activity_pairs_parallel_tools_without_inventing_completion_and_bounds_text():
    task = {}
    for name in ("a", "b"):
        update_activity(task, "tool_use", {"id": name, "name": "search"}, str)
    update_activity(task, "tool_result", {"id": "b", "content": "result"}, str)
    update_activity(task, "message_stop", {}, str)
    assert not task["activity"][0].get("done") and task["activity"][1]["done"]
    update_activity(task, "tool_result", {"id": "unknown", "content": "wrong"}, str)
    assert not task["activity"][0].get("done")
    for i in range(80):
        update_activity(task, "thinking", {"text": "x" * 6000}, str)
        update_activity(task, "message_stop", {}, str)
    assert len(task["activity"]) <= 48
    assert len(json.dumps(task["activity"], ensure_ascii=False).encode()) <= 24000
    assert any(e.get("id") == "a" for e in task["activity"])


def test_ambiguous_legacy_tool_results_are_visible_without_guessing_parallel_call_identity():
    task = {}
    for _ in range(2):
        update_activity(task, "tool_use", {"name": "http", "input": {"url": "https://example.test"}}, str)
    update_activity(task, "tool_result", {"name": "http", "content": "收到原始文献内容"}, str)
    assert not any(e.get("done") for e in task["activity"] if e["type"] == "tool")
    result = task["activity"][-1]
    assert result["type"] == "tool-result" and "原始文献" in result["resultDetail"]


async def test_running_task_does_not_generate_a_waiting_announcement(running, monkeypatch):
    gateway, provider, _, port = running
    release = asyncio.Event()

    async def blocked(*args, **kwargs):
        await release.wait()
        return "实际结果"

    monkeypatch.setattr(gateway.tasks.client, "run", blocked)
    client = await open_client(port)
    try:
        task = await gateway.tasks.submit("查找资料", "test-session")
        await client.wait("task.running")
        task["startedAt"] = now_ms() - 100000
        await gateway.tasks.report_progress(task, "正在等待工具")
        await asyncio.sleep(0.5)
        assert not any(e["type"] == "speech_text_buffer.commit" for e in provider.received)
        release.set()
        result = await client.wait("transcript.final", lambda e: e.get("origin") == "announcement")
        assert result["content"] == "实际结果"
    finally:
        release.set()
        await client.ws.close()


async def test_stall_error_is_reported_as_unconfirmed_tool_status(tmp_path):
    from gateway.chat import Chat
    from gateway.local_history import LocalHistory
    client = BeingClient(settings(tmp_path))
    history = LocalHistory(tmp_path / "history.sqlite3")
    try:
        message = client.error_message({"message": "LLM stream stalled: no data received for 60s"})
        assert "60 秒" in message and "中断" in message and "工具最终状态尚未确认" not in message
        chat = Chat(client, history)
        chat.current = chat.new_stream(settings(tmp_path).being_scene, "request", 1)
        chat.consume("error", {"message": "LLM stream stalled: no data received for 60s"})
        assert chat.current["failed"] and "Being 已中断本轮回复" in chat.events[-1]["data"]["message"]
    finally:
        await client.close()
        history.close()


async def test_recovery_rebuilds_activity_without_duplicate_thinking_or_tools(tmp_path):
    body = [("meta", {"stream_id": "research"}), ("thinking", {"text": "核对来源"}),
            ("tool_use", {"id": "search", "name": "web_search"}),
            ("tool_result", {"id": "search", "content": "真实结果"}),
            ("content_block_delta", {"delta": {"text": "资料说明"}}), ("message_stop", {})]
    async def respond(request):
        return httpx.Response(200, json={"stream_id": "research", "finished": True,
            "events": [{"seq": i + 1, "event": event, "data": data} for i, (event, data) in enumerate(body)]})
    async def emit(*_): pass
    store = Store(tmp_path / "state.sqlite3")
    manager = TaskManager(settings(tmp_path), store, emit)
    await manager.client.close()
    manager.client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond), store=store)
    try:
        task = await manager.submit("查找资料", "voice-session")
        task.update(status="recovering", being={"submitted": True, "stream_id": "research",
            "endpoint": manager.settings.being_endpoint, "scene_id": "desktop-scene"},
            activity=[{"type": "think", "id": "old", "text": "旧的部分记录"}])
        await manager.execute(task)
        assert task["status"] == "completed" and task["result"] == "资料说明"
        assert len(task["activity"]) == 2 and task["activity"][0]["text"] == "核对来源"
        assert task["activity"][1]["done"]
    finally:
        await manager.close()
        store.close()
