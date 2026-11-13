"""Corrections reach an active stream instead of waiting behind the original task."""
import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from gateway.being import BeingClient
from gateway.store import Store
from gateway.tasks import TaskManager
from gateway.tools import Tools
from test_being import settings, event

pytestmark = pytest.mark.asyncio


async def make_manager(tmp_path, respond):
    store = Store(tmp_path / "store.sqlite3")
    async def emit(*_): pass
    manager = TaskManager(settings(tmp_path), store, emit)
    await manager.client.close()
    manager.client = BeingClient(settings(tmp_path), store=store, transport=httpx.MockTransport(respond))
    return manager, store


@pytest.mark.parametrize("late_error", [False, True])
async def test_cancelled_task_drops_an_update_already_sent_to_being(tmp_path, late_error):
    manager, store = await make_manager(tmp_path, lambda _: httpx.Response(204))
    entered, release = asyncio.Event(), asyncio.Event()
    events = []

    async def update(*_):
        entered.set()
        await release.wait()
        if late_error:
            raise httpx.ReadError("late update failed")

    async def emit(_, event):
        events.append(event["type"])

    async def stop(_):
        pass

    manager.client.update = update
    manager.client.cancel = stop
    manager.emit = emit
    try:
        task = await manager.submit("原目标", "voice")
        task.update(status="running", being={"submitted": True})
        manager.active = task
        job = asyncio.create_task(manager.update(task["id"], "新目标", "voice", "t2"))
        await asyncio.wait_for(entered.wait(), 2)
        await manager.cancel(task["id"])
        release.set()
        assert await job == {"status": "discarded", "task_id": task["id"]}
        assert task["status"] == "cancelled" and task["objective"] == "原目标"
        assert events[-1] == "task.cancelled" and "task.updated" not in events
    finally:
        release.set()
        await manager.close()
        store.close()


@pytest.mark.parametrize("verbatim", [False, True])
async def test_queued_correction_reuses_task_and_submits_new_target(tmp_path, verbatim):
    sent = []
    async def respond(request):
        if request.method == "GET":
            return httpx.Response(204)
        sent.append(json.loads(request.content))
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=event("meta", {"stream_id": "s"}))
    manager, store = await make_manager(tmp_path, respond)
    try:
        original = "统计桌面 CSV 行列数"
        correction = "不是桌面目录，是本地磁盘 D"
        objective = correction if verbatim else "统计 D 盘 CSV 数量和每个文件行列数"
        task = await manager.submit(original, "voice")
        result = await manager.update(task["id"], objective, "voice", "t2", correction)
        assert result["status"] == "updated" and len(manager.tasks) == 1 and not sent
        await manager.execute(task)
        assert len(sent) == 1 and sent[0]["message"].startswith(objective)
        assert f"<!-- gateway-task: {task['id']} -->" in sent[0]["message"]
        assert sent[0]["message"].count(correction) == 1
        assert (original in sent[0]["message"]) is verbatim
    finally:
        await manager.close()
        store.close()


async def test_running_correction_does_not_wait_for_original_stream_or_duplicate_task(tmp_path):
    started, release = asyncio.Event(), asyncio.Event()
    sent = []
    class Live(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield event("meta", {"stream_id": "original"})
            started.set()
            await release.wait()
            yield event("content_block_delta", {"delta": {"text": "D 盘结果"}})
    async def respond(request):
        if request.method == "GET":
            return httpx.Response(200, json={"stream_id": "original", "scene_id": "desktop-scene", "finished": False}) if sent else httpx.Response(204)
        sent.append(json.loads(request.content))
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Live()) if len(sent) == 1 else httpx.Response(202)
    manager, store = await make_manager(tmp_path, respond)
    task = await manager.submit("统计桌面 CSV 行列数", "voice")
    job = asyncio.create_task(manager.execute(task))
    try:
        await asyncio.wait_for(started.wait(), 2)
        result = await asyncio.wait_for(manager.update(task["id"], "统计 D 盘 CSV 数量和行列数", "voice", "t2"), 1)
        assert result["status"] == "accepted" and not job.done()
        assert len(sent) == 2 and len(manager.tasks) == 1 and task["status"] == "running"
        assert sent[1]["scene_id"] == "desktop-scene" and "统计 D 盘" in sent[1]["message"]
        await manager.update(task["id"], "重复修正", "voice", "t2")
        assert len(sent) == 2
        release.set()
        await job
        assert task["result"] == "D 盘结果"
    finally:
        release.set()
        await job
        await manager.close()
        store.close()


@pytest.mark.parametrize("active", [None, {"stream_id": "other"}, {"stream_id": "original", "finished": True}, {"stream_id": "original", "scene_id": "other"}])
async def test_update_refuses_unowned_or_finished_stream(tmp_path, active):
    posts = []
    async def respond(request):
        if request.method == "POST": posts.append(request)
        return httpx.Response(200, json=active) if active else httpx.Response(204)
    manager, store = await make_manager(tmp_path, respond)
    try:
        task = await manager.submit("旧目标", "voice")
        task.update(status="running", being={"submitted": True, "stream_id": "original", "endpoint": manager.settings.being_endpoint, "scene_id": "desktop-scene"})
        with pytest.raises(ValueError):
            await manager.update(task["id"], "新目标", "voice", "t2")
        assert not posts and "lastUpdate" not in task and task["objective"] == "旧目标"
    finally:
        await manager.close()
        store.close()


@pytest.mark.parametrize("response", ["lost", "unexpected_200"])
async def test_uncertain_update_is_persisted_and_never_retried(tmp_path, response):
    posts = []
    async def respond(request):
        if request.method == "GET":
            return httpx.Response(200, json={"stream_id": "original", "finished": False})
        posts.append(request)
        if response == "lost": raise httpx.ReadError("lost")
        return httpx.Response(200)
    manager, store = await make_manager(tmp_path, respond)
    try:
        task = await manager.submit("旧目标", "voice")
        task.update(status="running", being={"submitted": True, "stream_id": "original", "endpoint": manager.settings.being_endpoint, "scene_id": "desktop-scene"})
        with pytest.raises((httpx.ReadError, RuntimeError)):
            await manager.update(task["id"], "新目标", "voice", "t2")
        assert store.get("tasks")[task["id"]]["lastUpdate"]["status"] == "unknown"
        result = await manager.update(task["id"], "新目标", "voice", "t2")
        assert result["status"] == "unknown"
        with pytest.raises(ValueError, match="尚待核对"):
            await manager.update(task["id"], "新目标", "voice", "t3")
        assert len(posts) == 1 and task["objective"] == "旧目标"
    finally:
        await manager.close()
        store.close()


async def test_tool_resolves_only_unambiguous_current_task_and_forwards_user_words(tmp_path):
    async def respond(_): raise AssertionError("queued updates must not send HTTP")
    manager, store = await make_manager(tmp_path, respond)
    try:
        tools = Tools(manager.settings, store, manager)
        session = SimpleNamespace(id="voice", turn="t2", client_tools={})
        task = await manager.submit("桌面 CSV", "voice")
        store.message("voice", "user", "不是桌面，是本地磁盘 D", turnId="t2")
        result = await tools.execute("update_agent_task", {"objective": "统计 D 盘 CSV 行列数"}, session)
        assert result["task_id"] == task["id"]
        assert task["lastUpdate"]["userText"] == "不是桌面，是本地磁盘 D"
        await manager.submit("其他工作", "voice")
        with pytest.raises(ValueError, match="唯一确定"):
            await tools.execute("update_agent_task", {"objective": "修正目标"}, session)
        with pytest.raises(ValueError, match="过期"):
            await tools.execute("update_agent_task", {"objective": "旧轮次"}, session, turn_id="t1")
        with pytest.raises(ValueError, match="当前场景"):
            await manager.update(task["id"], "其他会话", "other", "t3")
    finally:
        await manager.close()
        store.close()


@pytest.mark.parametrize("status", ["completed", "cancelled", "cancelling", "scheduled"])
async def test_update_rejects_inactive_or_stopping_work(tmp_path, status):
    async def respond(_): raise AssertionError("no HTTP should be sent")
    manager, store = await make_manager(tmp_path, respond)
    try:
        task = await manager.submit("原目标", "voice")
        task["status"] = status
        with pytest.raises(ValueError, match="不能修正"):
            await manager.update(task["id"], "新目标", "voice", "t2")
        assert task["objective"] == "原目标"
    finally:
        await manager.close()
        store.close()
