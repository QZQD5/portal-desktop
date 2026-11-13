import asyncio
import json

import httpx
import pytest

from gateway.being import BeingClient
from gateway.config import Settings
from gateway.store import Store
from gateway.tasks import TaskManager

pytestmark = pytest.mark.asyncio


def settings(tmp_path):
    return Settings(data_dir=tmp_path, being_endpoint="http://127.0.0.1/willow", being_token="private-token",
                    being_scene={"scene_id": "desktop-scene", "scene_meta": {"client": "portal-desktop"}})


def event(kind, data):
    return f"event: {kind}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode()


class Chunks(httpx.AsyncByteStream):
    def __init__(self, data):
        self.data = data

    async def __aiter__(self):
        for index in range(0, len(self.data), 3):
            yield self.data[index:index + 3]


async def test_split_utf8_sse_and_message_stop_is_not_task_completion(tmp_path):
    sent = []
    body = event("meta", {"stream_id": "stream-1"}) + event("content_block_delta", {"delta": {"text": "开始"}})
    body += event("message_stop", {"session_id": "session-1"})
    body += event("tool_use", {"name": "tool"}) + event("content_block_delta", {"delta": {"text": "完成"}})
    async def respond(request):
        assert request.url.params["token"] == "private-token"
        if request.method == "GET":
            return httpx.Response(204)
        sent.append(json.loads(request.content))
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Chunks(body))
    client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond))
    task = {"objective": "测试", "status": "running"}
    progress = []
    async def report(text): progress.append(text)
    try:
        context = "<conversation_context>" + json.dumps({"messages": ["背景" * 8000, "最新的文字结果"]}, ensure_ascii=False) + "</conversation_context>"
        assert await client.run(task, context, lambda: None, report) == "开始\n\n完成"
        assert len(sent) == 1 and sent[0]["scene_id"] == "desktop-scene"
        assert sent[0]["message"].endswith(context)
        assert task["being"]["stream_id"] == "stream-1" and progress
    finally:
        await client.close()


async def test_task_forwards_being_deltas_before_completion_and_archives_one_full_result(tmp_path):
    from gateway.local_history import LocalHistory
    release, first = asyncio.Event(), asyncio.Event()
    events = []
    class Live(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield event("meta", {"stream_id": "stream-live"})
            yield event("content_block_delta", {"delta": {"text": "内存"}})
            await release.wait()
            yield event("content_block_delta", {"delta": {"text": "实时读数"}})
            yield event("message_stop", {})
    async def respond(request):
        return httpx.Response(204) if request.method == "GET" else httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=Live())
    async def emit(_, value):
        events.append(value)
        if value["type"] == "task.output.delta":
            first.set()
    store = Store(tmp_path / "store.sqlite3")
    archive = LocalHistory(tmp_path / "history.sqlite3")
    store.attach_history(archive, settings(tmp_path).being_scene)
    manager = TaskManager(settings(tmp_path), store, emit)
    await manager.client.close()
    manager.client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond), store=store)
    task = await manager.submit("读取内存", "voice-session")
    job = asyncio.create_task(manager.execute(task))
    try:
        await asyncio.wait_for(first.wait(), 2)
        assert not job.done() and task["status"] == "running"
        assert [e["content"] for e in events if e["type"] == "task.output.delta"] == ["内存"]
        assert not archive.page()
        release.set()
        await job
        assert [e["content"] for e in events if e["type"] == "task.output.delta"] == ["内存", "内存实时读数"]
        rows = archive.page()
        assert len(rows) == 1 and rows[0]["content"] == "内存实时读数"
        assert rows[0]["voiceKey"] == "agent:" + task["id"] and rows[0]["voicePending"]
        task["notificationStatus"] = "delivered"
        await manager.publish(task)
        assert len(archive.page()) == 1 and archive.page()[0]["voicePending"] is False
    finally:
        release.set()
        await job
        await manager.close()
        store.close()
        archive.close()


async def test_202_is_not_success_and_is_never_resubmitted(tmp_path):
    calls = []
    async def respond(request):
        calls.append(request.method)
        return httpx.Response(204 if request.method == "GET" else 202)
    client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond))
    task = {"objective": "once", "status": "running"}
    async def report(_): pass
    try:
        with pytest.raises(RuntimeError, match="已接收"):
            await client.run(task, "", lambda: None, report)
        with pytest.raises(RuntimeError, match="不会自动重发"):
            await client.run(task, "", lambda: None, report)
        assert calls == ["GET", "POST"]
    finally:
        await client.close()


@pytest.mark.parametrize("payload,reason", [
    ({"message": "模型服务请求超时"}, "模型服务请求超时"),
    ({"error": {"code": "rate_limit", "message": "调用频率超限"}}, "rate_limit：调用频率超限"),
    ({"error": "检索服务暂时不可用"}, "检索服务暂时不可用"),
    ({"user_message": "当前模型不支持工具调用"}, "当前模型不支持工具调用"),
    ({"code": 503}, "503"),
    ({"error": True}, "未提供具体错误原因"),
])
async def test_sse_error_preserves_reason_in_failed_task_and_history(tmp_path, payload, reason, caplog):
    from gateway.local_history import LocalHistory
    body = event("meta", {"stream_id": "failed-search"}) + event("error", payload)
    async def respond(request):
        return httpx.Response(204) if request.method == "GET" else httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=Chunks(body))
    async def emit(*_): pass
    store = Store(tmp_path / "store.sqlite3")
    archive = LocalHistory(tmp_path / "history.sqlite3")
    store.attach_history(archive, settings(tmp_path).being_scene)
    manager = TaskManager(settings(tmp_path), store, emit)
    await manager.client.close()
    manager.client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond), store=store)
    try:
        task = await manager.submit("查找资料", "voice-session")
        await manager.execute(task)
        assert task["status"] == "failed" and not task.get("result")
        assert reason in task["error"]
        assert reason in archive.page()[0]["content"]
        assert reason in caplog.text and "failed-search" in caplog.text
        assert "请在聊天中查看详情" not in task["error"]
    finally:
        await manager.close()
        store.close()
        archive.close()


async def test_upstream_error_redacts_credentials_and_bounds_details(tmp_path, caplog):
    client = BeingClient(settings(tmp_path))
    async def progress(_): pass
    task = {"id": "secret-error", "being": {"stream_id": "failed"}}
    try:
        with pytest.raises(RuntimeError) as failure:
            await client.consume(task, "error", {"message":
                "private-token Bearer confidential-value sk-abcdefgh123456 "
                "https://example.test/private-token?token=secret-value\n" + "详细原因" * 400},
                [], lambda: None, progress)
        message = str(failure.value)
        for secret in ("private-token", "confidential-value", "sk-abcdefgh123456", "secret-value", "https://"):
            assert secret not in message and secret not in caplog.text
        assert len(message) < 650 and "\n" not in message
    finally:
        await client.close()


async def test_recovery_requires_exact_stream_and_keeps_authentication(tmp_path):
    async def respond(request):
        assert request.url.params["token"] == "private-token"
        return httpx.Response(200, json={"stream_id": "someone-else", "finished": True, "events": []})
    client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond))
    async def report(_): pass
    try:
        with pytest.raises(RuntimeError, match="不可回放"):
            await client.recover({"being": {"stream_id": "mine"}}, lambda: None, report)
        with pytest.raises(RuntimeError, match="不能安全取消"):
            await client.cancel({"being": {"submitted": True}})
    finally:
        await client.close()


async def test_shutdown_checkpoints_without_stopping_remote_task(tmp_path):
    store = Store(tmp_path / "state.sqlite3")
    async def emit(*_): pass
    manager = TaskManager(settings(tmp_path), store, emit)
    started = asyncio.Event()
    async def run(task, *_):
        task["being"] = {"submitted": True, "stream_id": "live", "endpoint": manager.settings.being_endpoint, "scene_id": "desktop-scene"}
        started.set()
        await asyncio.Future()
    async def cancelled(_): pytest.fail("Shutdown must not call /api/stop")
    manager.client.run = run
    manager.client.cancel = cancelled
    await manager.start()
    task = await manager.submit("continue remotely", "voice-session")
    await asyncio.wait_for(started.wait(), 2)
    await manager.close()
    assert task["status"] == "recovering"
    assert store.get("tasks")[task["id"]]["being"]["stream_id"] == "live"
    store.close()

@pytest.mark.parametrize("code,body,accepted", [
    (200, {"ok": True, "stream_id": "mine"}, True),
    (204, None, True),
    (200, {"ok": False}, False),
    (200, {"ok": True, "stream_id": "other"}, False),
    (200, {"stopped": False}, False),
    (200, {}, False),
    (202, {"ok": True}, False),
])
async def test_cancel_requires_confirmation_for_the_owned_stream(tmp_path, code, body, accepted):
    async def respond(request):
        assert request.url.path == "/willow/api/stop"
        assert json.loads(request.content) == {"stream_id": "mine"}
        return httpx.Response(code, json=body) if body is not None else httpx.Response(code)
    client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond))
    try:
        task = {"being": {"submitted": True, "stream_id": "mine"}}
        if accepted:
            await client.cancel(task)
        else:
            with pytest.raises(RuntimeError):
                await client.cancel(task)
    finally:
        await client.close()


@pytest.mark.parametrize("body", [b"", b"Stopped", b"<html>OK</html>"])
@pytest.mark.parametrize("status,accepted", [
    (None, True),
    ({"stream_id": "mine", "finished": True}, True),
    ({"stream_id": "mine", "finished": False}, False),
    ({"stream_id": "other", "finished": True}, False),
    ({"stream_id": "mine", "finished": "true"}, False),
    ({}, False),
])
async def test_non_json_stop_receipt_requires_stream_confirmation(tmp_path, body, status, accepted):
    calls = []
    async def respond(request):
        calls.append((request.method, request.url.path))
        assert request.url.params["token"] == "private-token"
        if request.method == "POST":
            assert json.loads(request.content) == {"stream_id": "mine"}
            return httpx.Response(200, content=body)
        return httpx.Response(204) if status is None else httpx.Response(200, json=status)
    client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond))
    try:
        task = {"being": {"submitted": True, "stream_id": "mine"}}
        if accepted:
            await client.cancel(task)
        else:
            with pytest.raises(RuntimeError, match="未确认停止"):
                await client.cancel(task)
        assert calls == [("POST", "/willow/api/stop"), ("GET", "/willow/api/stream/active")]
    finally:
        await client.close()


@pytest.mark.parametrize("body", [b"null", b"<html>Unavailable</html>"])
async def test_invalid_status_cannot_confirm_a_non_json_stop_receipt(tmp_path, body):
    async def respond(request):
        return httpx.Response(200, content=b"" if request.method == "POST" else body)
    client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond))
    try:
        with pytest.raises(RuntimeError, match="未确认停止"):
            await client.cancel({"being": {"submitted": True, "stream_id": "mine"}})
    finally:
        await client.close()


@pytest.mark.parametrize("outcome", ["", "已完成", RuntimeError("stream closed")])
@pytest.mark.parametrize("confirmed", [True, False])
async def test_stream_end_waits_for_pending_cancellation_before_publishing(tmp_path, outcome, confirmed):
    store = Store(tmp_path / "state.sqlite3")
    events = []
    running, ended, stopping, confirm, next_done = (asyncio.Event() for _ in range(5))
    async def emit(_, value):
        events.append(value)
        if value["type"] == "task.completed" and value["task"]["objective"] == "next":
            next_done.set()
    manager = TaskManager(settings(tmp_path), store, emit)
    task = await manager.submit("cancel me", "voice-session")
    following = await manager.submit("next", "voice-session")
    async def run(current, *_):
        if current is following:
            return "下一条完成"
        running.set()
        await ended.wait()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome
    async def cancel(_):
        stopping.set()
        await confirm.wait()
        if not confirmed:
            raise RuntimeError("Stop not confirmed")
    manager.client.run, manager.client.cancel = run, cancel
    await manager.start()
    try:
        await asyncio.wait_for(running.wait(), 2)
        cancellation = manager.start_cancel(task["id"])
        await asyncio.wait_for(stopping.wait(), 2)
        reader = manager.execution
        ended.set()
        await asyncio.gather(reader, return_exceptions=True)
        await asyncio.sleep(0)
        assert task["status"] == "cancelling"
        assert following["status"] == "queued"
        assert not store.history("voice-session")
        confirm.set()
        if confirmed:
            await cancellation
        else:
            with pytest.raises(RuntimeError, match="not confirmed"):
                await cancellation
        await asyncio.wait_for(next_done.wait(), 2)
        expected = "cancelled" if confirmed else "failed" if isinstance(outcome, Exception) else "completed"
        assert task["status"] == expected
        assert task["discard_output"] and task["notificationStatus"] == "none"
        assert store.get("tasks")[task["id"]]["status"] == expected
        terminal = {e["type"] for e in events if e.get("task", {}).get("id") == task["id"]
                    and e["type"] in {"task.failed", "task.completed", "task.cancelled"}}
        assert terminal == {"task." + expected}
        assert not any(row.get("taskId") == task["id"] for row in store.history("voice-session"))
    finally:
        ended.set()
        confirm.set()
        await manager.close()
        store.close()


async def test_failed_cancel_restores_tracking_and_does_not_abort_reader(tmp_path):
    store = Store(tmp_path / "state.sqlite3")
    async def emit(*_): pass
    manager = TaskManager(settings(tmp_path), store, emit)
    task = await manager.submit("keep tracking", "voice-session")
    manager.active = task
    task["status"] = "running"
    manager.execution = asyncio.create_task(asyncio.Event().wait())
    async def reject(_): raise RuntimeError("Stop not confirmed")
    manager.client.cancel = reject
    try:
        with pytest.raises(RuntimeError, match="not confirmed"):
            await manager.cancel(task["id"])
        assert task["status"] == "running" and not manager.execution.done()
    finally:
        manager.execution.cancel()
        await asyncio.gather(manager.execution, return_exceptions=True)
        manager.active = manager.execution = None
        await manager.close()
        store.close()


async def test_voice_task_does_not_capture_web_replies_from_a_shared_stream(tmp_path):
    body = b"".join(event(kind, data) for kind, data in [
        ("meta", {"stream_id": "shared", "scene_id": "desktop-scene"}),
        ("content_block_delta", {"delta": {"text": "自己的结果"}}),
        ("message_stop", {}),
        ("meta", {"continuation": True, "scene_id": "web-scene"}),
        ("content_block_delta", {"delta": {"text": "别人的网页对话"}}),
        ("message_stop", {}),
    ])
    async def respond(req):
        if req.method == "GET":
            return httpx.Response(204)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Chunks(body))
    client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond))
    async def report(_): pass
    try:
        result = await client.run({"objective": "任务", "status": "running"}, "", lambda: None, report)
        assert result == "自己的结果"
    finally:
        await client.close()
