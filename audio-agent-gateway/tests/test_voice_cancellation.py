import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from conftest import open_client
from test_being import event
from test_voice_requests import native, speech, call, model_tool  # noqa: F401


@pytest.mark.parametrize("stop_status", [204, 202])
async def test_model_cancel_tool_targets_current_text_stream(native, monkeypatch, stop_status):
    gateway = native.gateway
    chat = gateway.chat
    current = chat.current = chat.new_stream(gateway.settings.being_scene, "text", 1)
    current["upstream_id"] = "owned-text-stream"
    chat.save()
    send_stop = AsyncMock(return_value=httpx.Response(stop_status))
    monkeypatch.setattr(chat.client.http, "post", send_stop)

    await speech(native, "cancel", "好了，不用查了。")
    send_stop.assert_not_called()
    await call(native, "cancel-reply", "cancel_agent_task", {"stream_id": current["stream_id"]})
    await asyncio.gather(*native.tool_tasks)
    assert native.responses["cancel-reply"]["calls"]["cancel-reply-call"] == {
        "status": "accepted", "stream_id": current["stream_id"],
    }
    await asyncio.gather(*chat.voice_stops.values())
    send_stop.assert_awaited_once_with("api/stop", json={"stream_id": "owned-text-stream"}, timeout=15)
    assert current["discard_output"] and bool(current.get("stopped")) == (stop_status == 204)
    assert native.queue.empty()


@pytest.mark.parametrize("mode", ["spliced", "other_scene", "ambiguous"])
async def test_model_cancel_tool_checks_scope_and_target(native, monkeypatch, mode):
    gateway = native.gateway
    chat = gateway.chat
    scene = {"scene_id": "other"} if mode == "other_scene" else gateway.settings.being_scene
    current = chat.current = chat.new_stream(scene, "text", 1)
    current["upstream_id"] = "owned-text-stream"
    chat.save()
    send_stop = AsyncMock()
    monkeypatch.setattr(chat.client.http, "post", send_stop)
    if mode == "ambiguous":
        await gateway.tasks.submit("另一项工作", native.id)
    await speech(native, "cancel", "取消刚才的任务。")
    if mode == "spliced":
        chat.request_order += 1
    args = {} if mode == "ambiguous" else {"stream_id": current["stream_id"]}
    await call(native, "cancel-reply", "cancel_agent_task", args)
    await asyncio.gather(*native.tool_tasks)
    assert native.responses["cancel-reply"]["calls"]["cancel-reply-call"]["error"]
    send_stop.assert_not_called()
    assert not current.get("discard_output")


async def test_voice_cancellation_acknowledges_before_stop_receipt_and_discards_late_task_result(running, monkeypatch):
    gateway, provider, _, port = running
    started, release = asyncio.Event(), asyncio.Event()
    stops = []
    finish = provider.finish
    async def acknowledge(ws, state, rid, text):
        await finish(ws, state, rid, "我这就帮你取消。" if text == "tool completed" else text)
    async def run(task, *args):
        started.set()
        await release.wait()
        return "查询结果"
    async def cancel(task):
        stops.append(task["id"])
        await release.wait()
        raise RuntimeError("尚未确认停止")
    model_tool(monkeypatch, provider, {"好了，不用查了。": ("cancel_agent_task", {})})
    monkeypatch.setattr(provider, "finish", acknowledge)
    monkeypatch.setattr(gateway.tasks.client, "run", run)
    monkeypatch.setattr(gateway.tasks.client, "cancel", cancel)
    client = await open_client(port)
    try:
        task = await gateway.tasks.submit("查询出处和含义", gateway.active.id)
        await asyncio.wait_for(started.wait(), 2)
        await client.speak(provider, "好了，不用查了。")
        await client.wait("transcript.final", lambda e: e.get("role") == "assistant" and e["content"] == "我这就帮你取消。")
        assert task["status"] == "cancelling" and stops == [task["id"]]
        release.set()
        result = await client.wait("task.completed")
        assert result["task"]["result"] is None and result["task"]["notificationStatus"] == "none"
        assert not any(row["content"] == "查询结果" for row in gateway.history.page())
        assert not any(e.get("role") == "assistant" and "停止查询了" in e.get("content", "")
                       for e in client.events)
    finally:
        release.set()
        await client.ws.close()


async def test_voice_cancel_discards_result_completed_while_model_is_selecting_tool(running, monkeypatch):
    gateway, provider, http, port = running
    started, finish_work, routing, release_cancel = (asyncio.Event() for _ in range(4))
    objective = "帮我查一下桌面有哪些文件"
    result = "桌面清点的结果"
    other_result = "另一项工作的结果"
    execute = gateway.tools.execute

    async def delayed_cancel(name, args, session, **options):
        if name == "cancel_agent_task":
            routing.set()
            await release_cancel.wait()
        return await execute(name, args, session, **options)

    async def being(task, context, save, progress, output):
        if task["objective"] == objective:
            started.set()
            await finish_work.wait()
            await output(result)
            return result
        return other_result

    model_tool(monkeypatch, provider, {"取消这个任务。": ("cancel_agent_task", {})})
    monkeypatch.setattr(gateway.tools, "execute", delayed_cancel)
    monkeypatch.setattr(gateway.tasks.client, "run", being)
    cancel_backend = AsyncMock()
    monkeypatch.setattr(gateway.tasks.client, "cancel", cancel_backend)
    client = await open_client(port)
    posting = asyncio.create_task(http.post("/api/heart/chat/stream", json={
        "message": objective, "scene_id": gateway.settings.being_scene["scene_id"],
    }))
    try:
        await asyncio.wait_for(started.wait(), 2)
        task, = gateway.tasks.tasks.values()
        await client.speak(provider, "取消这个任务。")
        await asyncio.wait_for(routing.wait(), 2)
        finish_work.set()
        response = await asyncio.wait_for(posting, 2)
        assert result in response.text and task["status"] == "completed"
        assert f'"taskId": "{task["id"]}"' in response.text
        assert task["notificationStatus"] == "pending"
        stale_completion = {"type": "task.completed", "task": dict(task)}

        unrelated = await gateway.tasks.submit("另一项工作", gateway.active.id)
        release_cancel.set()
        discarded = await client.wait("task.output.discarded")
        assert discarded["task"]["id"] == task["id"]
        assert task["discard_output"] and task["notificationStatus"] == "none"
        assert gateway.discarded_task_event(stale_completion)
        assert gateway.history.find("task-result:" + task["id"]) is None
        assert gateway.chat.current["discard_output"]
        assert not any(
            item["event"] == "content_block_delta" and gateway.chat.visible_event(gateway.chat.current, item)
            for item in gateway.chat.events
        )
        await client.wait("task.completed", lambda event: event["task"]["id"] == unrelated["id"])
        await client.wait("transcript.final", lambda event: event.get("content") == other_result)
        assert not unrelated.get("discard_output")
        assert not any(event["type"] == "speech_text_buffer.commit" and result in event.get("text", "")
                       for event in provider.received)
        cancel_backend.assert_not_called()
    finally:
        release_cancel.set()
        finish_work.set()
        await asyncio.gather(posting, return_exceptions=True)
        await client.ws.close()


async def test_voice_cancel_discards_text_stream_while_stop_is_unconfirmed(running, monkeypatch):
    gateway, provider, http, port = running
    opened, stopping, release_text, release_stop = [asyncio.Event() for _ in range(4)]
    send = gateway.tasks.client.http.send
    finish = provider.finish

    async def stale_reply(ws, state, response_id, text):
        if text == "tool completed":
            text = {
                "取消任务。": "我这就帮你取消。",
                "现在有哪些任务？": "当前场景没有正在处理或排队的任务。",
            }[state["text"]]
        await finish(ws, state, response_id, text)

    class Live(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield event("meta", {"stream_id": "original-text"})
            opened.set()
            await release_text.wait()
            yield event("tool_use", {"name": "late tool"})
            yield event("content_block_delta", {"delta": {"text": "不得播报的结果"}})
            yield event("message_stop", {})

    async def respond(request, **kwargs):
        if request.url.path.endswith("/api/chat/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Live())
        if request.url.path.endswith("/api/stop"):
            assert json.loads(request.content) == {"stream_id": "original-text"}
            stopping.set()
            await release_stop.wait()
            return httpx.Response(202)
        return await send(request, **kwargs)

    monkeypatch.setattr(gateway.tasks.client.http, "send", respond)
    model_tool(monkeypatch, provider, {
        "取消任务。": ("cancel_agent_task", {}),
        "现在有哪些任务？": ("get_agent_task_status", {"list_all": True}),
    })
    monkeypatch.setattr(provider, "finish", stale_reply)
    client = await open_client(port)
    posting = asyncio.create_task(http.post("/api/heart/chat/stream", json={"message": "查一下内存", "scene_id": "fixture-scene"}))
    try:
        await asyncio.wait_for(opened.wait(), 2)
        await client.speak(provider, "取消任务。")
        ack = await client.wait("transcript.final", lambda e: e.get("content") == "我这就帮你取消。")
        await client.wait("audio.done", lambda e: e.get("responseId") == ack["responseId"])
        await asyncio.wait_for(stopping.wait(), 2)
        task = next(iter(gateway.tasks.tasks.values()))
        assert not release_stop.is_set() and task["discard_output"]
        release_text.set()
        response = await asyncio.wait_for(posting, 2)
        assert "event: stream_discarded" in response.text
        assert "不得播报的结果" not in response.text and "late tool" not in response.text
        assert not any(row["role"] == "assistant" and "不得播报" in row["content"] for row in gateway.history.page())
        assert not gateway.chat.current.get("stopped")
        release_stop.set()
        await client.speak(provider, "现在有哪些任务？")
        answer = await client.wait("transcript.final", lambda e: e.get("content") == "当前场景没有正在处理或排队的任务。")
        assert answer["origin"] == "model"
        await client.wait("audio.done", lambda e: e.get("responseId") == answer["responseId"])
        assert not any("36.5%" in row["content"] for row in gateway.history.page())
    finally:
        release_stop.set()
        release_text.set()
        await asyncio.gather(posting, return_exceptions=True)
        await client.ws.close()


async def test_task_list_keeps_results_out_and_uses_only_current_scene(native):
    gateway = native.gateway
    active = await gateway.tasks.submit("查一下天气", "previous-voice-session")
    foreign = await gateway.tasks.submit("另一个场景的运行任务", native.id)
    foreign["scene"] = {"scene_id": "other"}
    old = await gateway.tasks.submit("旧硬件分析", native.id)
    old.update(status="completed", result="内存 36.5%")
    muted = await gateway.tasks.submit("刚取消的检查", native.id)
    muted.update(status="cancelled", discard_output=True, activity=[{"result": "隐藏结果"}])
    chat = gateway.chat
    chat.current = chat.new_stream(gateway.settings.being_scene, "typed-work", 1)
    chat.current["progress"] = "文字流旧输出"
    chat.save()
    await speech(native, "list", "你现在有几个后台任务？")
    assert native.queue.empty()
    await call(native, "list-reply", "get_agent_task_status", {"list_all": True})
    await asyncio.gather(*native.tool_tasks)
    listed = native.responses["list-reply"]["calls"]["list-reply-call"]
    assert [task["id"] for task in listed["tasks"]] == [active["id"]]
    assert listed["textWork"]["status"] == "running" and "progress" not in listed["textWork"]
    assert "result" not in listed["tasks"][0]
    muted_status = await gateway.tools.execute("get_agent_task_status", {"task_id": muted["id"]}, native)
    assert muted_status["status"] == "cancelled" and "activity" not in muted_status
    with pytest.raises(ValueError, match="当前场景没有这项任务"):
        await gateway.tools.execute("get_agent_task_status", {"task_id": foreign["id"]}, native)


@pytest.mark.parametrize("arguments", [{}, {"list_all": "True"}])
async def test_status_query_excludes_cancelled_and_discarded_tasks(native, arguments):
    manager = native.gateway.tasks
    task = await manager.submit("制作北极熊骑单车动画", native.id)
    await manager.cancel(task["id"])
    stopping = await manager.submit("另一项正在停止的任务", native.id)
    stopping.update(status="cancelling", discard_output=True)

    await speech(native, "list-after-cancel", "你现在还有其他任务吗？")
    await call(native, "list-after-cancel-reply", "get_agent_task_status", arguments)
    await asyncio.gather(*native.tool_tasks)
    receipt = native.responses["list-after-cancel-reply"]["calls"]["list-after-cancel-reply-call"]
    assert receipt == {"tasks": [], "textWork": None, "active_count": 0, "queued_count": 0,
                       "status": "empty", "user_message": "现在没有正在处理或排队的任务。"}
