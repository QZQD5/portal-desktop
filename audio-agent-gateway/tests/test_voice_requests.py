import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from conftest import open_client
from test_seeduplex import native as native  # noqa: PLC0414 - re-export the shared pytest fixture


async def speech(session, item, text, *, final=True):
    for raw in [
        {
            "type": "conversation.item.input_audio_transcription.started",
            "item_id": item,
        },
        {
            "type": "conversation.item.input_audio_transcription.delta",
            "item_id": item,
            "delta": text,
        },
        *(
            [
                {
                    "type": "conversation.item.input_audio_transcription.completed",
                    "item_id": item,
                    "text": text,
                }
            ]
            if final
            else []
        ),
    ]:
        for event in session.protocol.normalize(raw):
            await session.provider_event(event)
    return session.turn


async def call(session, rid, name, arguments):
    for event in session.protocol.normalize(
        {
            "type": "response.function_call_arguments.done",
            "response_id": rid,
            "name": name,
            "call_id": rid + "-call",
            "arguments": json.dumps(arguments),
        }
    ):
        await session.provider_event(event)


def model_tool(monkeypatch, provider, tools):
    reply = provider.reply

    async def call_tool(ws, state, *, native=False):
        if not native or state["text"] not in tools:
            return await reply(ws, state, native=native)
        name, arguments = tools[state["text"]]
        rid = "model-" + name
        state.update(active=rid, waiting_tool=True)
        await ws.send(json.dumps({
            "type": "response.function_call_arguments.done", "response_id": rid,
            "call_id": rid + "-call", "name": name, "arguments": json.dumps(arguments),
        }))

    monkeypatch.setattr(provider, "reply", call_tool)


@pytest.mark.parametrize("confirmed", [True, False])
async def test_interrupted_progress_query_does_not_bypass_model_tool_calls(native, confirmed):
    task = await native.gateway.tasks.submit("原来的工作", native.id)
    query_turn = await speech(native, "query", "到哪一步了", final=confirmed)
    cancel_turn = await speech(native, "cancel", "取消任务")
    await call(native, "cancel-response", "cancel_agent_task", {"task_id": task["id"]})
    await asyncio.gather(*native.tool_tasks)

    history = native.store.history(native.id)
    assert [m["content"] for m in history if m["role"] == "user"] == [
        "到哪一步了",
        "取消任务",
    ]
    assert not any(m["role"] == "assistant" and m["turnId"] == query_turn and "还在排队" in m["content"] for m in history)
    await asyncio.gather(*native.gateway.tasks.cancellations.values())
    assert task["status"] == "cancelled"
    assert query_turn != cancel_turn
    assert len(native.gateway.tasks.tasks) == 1


async def test_confirmed_unanswered_request_stays_in_context_without_delegation(
    native,
):
    first = await speech(native, "first", "整理下载目录")
    await speech(native, "second", "另一个问题", final=False)
    await speech(native, "third", "再问一个问题", final=False)

    tasks = list(native.gateway.tasks.tasks.values())
    assert tasks == []
    assert any(m["content"] == "整理下载目录" and m["turnId"] == first for m in native.store.history(native.id))


@pytest.mark.parametrize("name", ["spawn_thinking", "cancel_agent_task"])
@pytest.mark.parametrize("next_input", ["speech", "text"])
async def test_accepted_tool_survives_next_input_with_original_turn(native, name, next_input):
    task = await native.gateway.tasks.submit("已有工作", native.id)
    first = await speech(native, "first", "取消刚才的任务" if name == "cancel_agent_task" else "继续处理")
    arguments = (
        {"objective": "制作报表"}
        if name == "spawn_thinking"
        else {"task_id": task["id"]}
    )
    await call(native, "old-response", name, arguments)
    if next_input == "speech":
        await speech(native, "second", "到哪一步了")
    else:
        await native.input([{"type": "text", "text": "到哪一步了"}])
    await asyncio.gather(*native.tool_tasks)

    result = native.responses["old-response"]["calls"]["old-response-call"]
    assert not result.get("error")
    if name == "spawn_thinking":
        created = native.gateway.tasks.tasks[result["task_id"]]
        assert created["turnId"] == first
        assert created["objective"] == "制作报表"
        context = native.store.get("task_context:" + created["id"])
        assert context["messages"][-1]["content"] == "继续处理"
    else:
        await asyncio.gather(*native.gateway.tasks.cancellations.values())
        assert task["status"] == "cancelled"
    assert first in native.tool_turns
    assert native.turn not in native.tool_turns
    if name == "spawn_thinking":
        assert not any(
            m["role"] == "assistant" and m["turnId"] == first
            for m in native.store.history(native.id)
        )


async def test_completed_reply_is_not_submitted_again(native):
    await speech(native, "first", "你好")
    for raw in [
        {"type": "response.output_text.done", "response_id": "reply", "text": "你好！"},
        {"type": "response.done", "response_id": "reply"},
    ]:
        for event in native.protocol.normalize(raw):
            await native.provider_event(event)
    await speech(native, "second", "下一句")
    assert not native.gateway.tasks.tasks


async def test_unconfirmed_action_is_explained_without_execution(native):
    first = await speech(native, "first", "删除文件", final=False)
    await speech(native, "second", "你好")
    assert not native.gateway.tasks.tasks
    assert any(
        m["role"] == "assistant"
        and m["turnId"] == first
        and "识别没有完成" in m["content"]
        for m in native.store.history(native.id)
    )


@pytest.mark.parametrize("tool,text,args", [
    ("memory", "记住，你以后叫小红", {"action": "append", "document": "user", "content": "助手叫小红"}),
    ("spawn_thinking", "给吴友杰发私信", {"objective": "给 t_rsm8iP 发私信"}),
    ("update_being_memory", "记住，你以后叫小红", {"objective": "把 Being 的显示名改为小红"}),
])
async def test_provisional_transcript_cannot_write_or_delegate(native, tool, text, args):
    native.settings.backend_enabled = True
    await speech(native, "pending", text, final=False)
    await call(native, "premature", tool, args)
    await asyncio.gather(*native.tool_tasks)
    assert native.responses["premature"]["calls"]["premature-call"]["error"]
    assert not native.gateway.tasks.tasks
    assert not (native.settings.data_dir / "USER.md").exists()


async def test_scheduled_voice_task_keeps_original_transcript(native):
    native.settings.backend_enabled = True
    text = "明天帮我查一下南京的天气。"
    await speech(native, "schedule", text)
    await call(native, "schedule-reply", "schedule_reminder", {
        "execute_at": "2099-02-01T10:00:00+08:00", "type": "task", "reminder": "查询上海的天气",
    })
    await asyncio.gather(*native.tool_tasks)
    task, = native.gateway.tasks.tasks.values()
    assert task["status"] == "scheduled" and task["objective"] == "查询上海的天气"


@pytest.mark.parametrize("arguments", [{}, {"all": True}])
async def test_accepted_implicit_cancel_cannot_target_a_later_task(native, arguments):
    old = await native.gateway.tasks.submit("原来的工作", native.id)
    await speech(native, "cancel", "取消任务")
    await call(native, "cancel-response", "cancel_agent_task", arguments)
    await speech(native, "next", "新的工作")
    newer = await native.gateway.tasks.submit("新的工作", native.id, native.turn)
    await asyncio.gather(*native.tool_tasks)
    await asyncio.gather(*native.gateway.tasks.cancellations.values())
    assert old["status"] == "cancelled"
    assert newer["status"] == "queued"


async def test_unanswered_confirmed_cancel_does_not_cancel_without_model_tool_call(native):
    task = await native.gateway.tasks.submit("原来的工作", native.id)
    turn = await speech(native, "cancel", "取消任务")
    await speech(native, "next", "你好")
    assert task["status"] == "queued"
    assert len(native.gateway.tasks.tasks) == 1
    assert not any(
        m["role"] == "assistant" and m["turnId"] == turn and "已取消" in m["content"]
        for m in native.store.history(native.id)
    )


async def test_voice_delegation_sends_original_transcript_to_being(running, monkeypatch):
    gateway, _, _, port = running
    send = gateway.tasks.client.http.send
    sent = []

    async def observe(request, **kwargs):
        if request.method == "POST" and request.url.path.endswith("/api/chat/stream"):
            sent.append(json.loads(request.content)["message"])
        return await send(request, **kwargs)

    monkeypatch.setattr(gateway.tasks.client.http, "send", observe)
    client = await open_client(port)
    try:
        session = gateway.active
        text = "给吴友杰发一条私信，打个招呼。"
        model = "给 t_rsm8iP（吴悠杰）发私信"
        first = await speech(session, "first", text)
        assert not gateway.tasks.tasks
        await call(session, "delegated", "spawn_thinking", {"objective": model})
        await asyncio.gather(*session.tool_tasks)
        await speech(session, "second", "到哪一步了", final=False)
        completed = await client.wait("task.completed")
        assert completed["task"]["turnId"] == first
        assert completed["task"]["status"] == "completed"
        assert completed["task"]["objective"] == model
        assert len(sent) == 1 and sent[0].split("\n\n", 1)[0] == model
        assert completed["task"]["result"]
        assert len(gateway.tasks.tasks) == 1
    finally:
        await client.ws.close()


async def test_tool_result_is_retained_when_speech_is_interrupted_before_summary(
    native,
):
    await native.gateway.tasks.submit("原来的工作", native.id)
    query = await speech(native, "query", "到哪一步了")
    await call(native, "query-response", "get_agent_task_status", {})
    await asyncio.gather(*native.tool_tasks)
    await speech(native, "cancel", "取消任务")
    await speech(native, "next", "你好")
    replies = [
        m
        for m in native.store.history(native.id)
        if m["role"] == "assistant" and m["turnId"] == query
    ]
    assert not replies
    receipt = native.responses["query-response"]["calls"]["query-response-call"]
    assert receipt["status"] == "queued"


async def test_cancelled_task_invalidates_in_flight_status_reply(native):
    manager = native.gateway.tasks
    task = await manager.submit("原来的工作", native.id)
    await speech(native, "query", "到哪一步了")
    await call(native, "query-response", "get_agent_task_status", {"task_id": task["id"]})
    await asyncio.gather(*native.tool_tasks)
    assert native.responses["query-response"]["calls"]["query-response-call"]["status"] == "queued"

    await manager.cancel(task["id"])
    await native.discard_cancelled_replies()
    await native.provider_event({
        "type": "response.output_text.done", "response_id": "query-response", "text": "任务还在排队。",
    })
    assert native.responses["query-response"]["suppressed"]
    assert not any(row["content"] == "任务还在排队。" for row in native.store.history(native.id))


async def test_final_asr_with_different_item_id_preserves_context_without_delegation(native):
    turn = await speech(native, "start", "整理报告", final=False)
    for event in native.protocol.normalize(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "final",
            "text": "整理报告",
        }
    ):
        await native.provider_event(event)
    await speech(native, "next", "你好")
    tasks = list(native.gateway.tasks.tasks.values())
    assert tasks == []
    assert any(m["turnId"] == turn and m["content"] == "整理报告" for m in native.store.history(native.id))


@pytest.mark.parametrize("text", [
    "好了，不用说了。", "请先安静一下", "停。", "不要说了，帮我检查磁盘", "不要停止播报",
])
async def test_transcription_does_not_classify_stop_intent(native, text):
    await speech(native, "request", text)
    await native.provider_event({"type": "response.created", "response": {"id": "answer"}})
    assert not native.responses["answer"]["suppressed"]
    assert not native.gateway.tasks.tasks
    assert native.queue.empty()


async def test_barge_in_preserves_a_previously_accepted_delegation(native):
    await speech(native, "work", "整理报告")
    await call(native, "accepted", "spawn_thinking", {"objective": "整理报告"})
    await native.stop_playback_on_speech()
    await speech(native, "next", "好了，不用说了")
    await asyncio.gather(*native.tool_tasks)
    tasks = list(native.gateway.tasks.tasks.values())
    assert len(tasks) == 1 and tasks[0]["objective"] == "整理报告"
    assert tasks[0]["status"] == "queued"
    assert native.queue.empty()


async def test_late_native_acceptance_is_suppressed_after_delegation(native):
    await speech(native, "work", "查一下私信")
    await call(native, "accepted", "spawn_thinking", {"objective": "查一下私信"})
    await asyncio.gather(*native.tool_tasks)
    await native.provider_event({"type": "response.done", "response_id": "accepted"})
    await native.provider_event({"type": "response.created", "response": {"id": "late-acceptance"}})
    await native.provider_event({
        "type": "response.output_text.done", "response_id": "late-acceptance", "text": "任务已受理。",
    })
    assert native.responses["late-acceptance"]["suppressed"]
    assert not any(row["content"] == "任务已受理。" for row in native.store.history(native.id))


async def test_cancel_discards_same_response_tool_continuation(native, monkeypatch):
    await speech(native, "work", "查一下私信")
    await call(native, "accepted", "spawn_thinking", {"objective": "查一下私信"})
    await asyncio.gather(*native.tool_tasks)
    task = next(iter(native.gateway.tasks.tasks.values()))
    socket = AsyncMock()
    monkeypatch.setattr(native, "ws", socket)
    async with native.client_send_lock:
        await native.gateway.send(native, {"type": "audio.delta", "responseId": "accepted", "audio": "AAAAAA=="})
        cancellation = native.gateway.tasks.start_cancel(task["id"])
    await asyncio.gather(*native.client_send_tasks)
    socket.send_text.assert_not_awaited()
    assert task["discard_output"]
    await native.provider_event({
        "type": "response.output_audio_transcript.done", "response_id": "accepted", "text": "任务已受理。",
    })
    await native.provider_event({
        "type": "response.output_audio.delta", "response_id": "accepted", "delta": "AAAAAA==",
    })
    await native.discard_cancelled_replies()
    await cancellation
    assert native.responses["accepted"]["suppressed"]
    assert not any(event.args[0].get("type") in {"transcript.final", "audio.delta"}
                   and event.args[0].get("responseId") == "accepted" for event in native.emit.call_args_list)
    assert not any(row["content"] == "任务已受理。" for row in native.store.history(native.id))


@pytest.mark.parametrize("final", [False, True])
async def test_barge_in_does_not_repeat_receipts_for_the_announced_task(native, final):
    await speech(native, "work", "查一下私信")
    await call(native, "accepted", "spawn_thinking", {"objective": "查一下私信"})
    await asyncio.gather(*native.tool_tasks)
    await native.provider_event({"type": "response.done", "response_id": "accepted"})
    task = next(iter(native.gateway.tasks.tasks.values()))
    task.update(status="completed", result="没有收到私信。", notificationStatus="pending")

    native.settings.announcement_batch_window = 0
    await native.queue_announcements()
    reply = native.queue.get_nowait()
    native.queue.task_done()
    native.replies[reply.request_id] = reply
    await native.provider_event({"type": "response.created", "response": {
        "id": "result", "metadata": {"audio_agent_gateway_request_id": reply.request_id},
    }})
    await native.playback_started("result")
    await speech(native, "stop", "好了", final=final)

    assert native.responses["result"]["playback_status"] == "interrupted"
    assert task["status"] == "completed" and task["notificationStatus"] == "delivered"
    assert list(native.gateway.tasks.tasks) == [task["id"]]
    assert not any(
        event.args[0].get("responseId") == "tool:accepted-call"
        for event in native.emit.call_args_list
    )
    await native.queue_announcements()
    assert native.queue.empty()


async def test_websocket_text_keeps_simultaneous_speech_as_context(native):
    turn = await speech(native, "speaking", "绿色", final=False)
    await native.input([{"type": "text", "text": "改成蓝色"}])
    for event in native.protocol.normalize({"type": "conversation.item.input_audio_transcription.completed",
                                            "item_id": "speaking", "text": "先考虑绿色"}):
        await native.provider_event(event)
    history = native.store.history(native.id)
    assert any(row["content"] == "先考虑绿色" and row["turnId"] == turn and row["source"] == "voice-user-delayed" for row in history)
    assert len(native.gateway.tasks.tasks) == 1
    await native.input([{"type": "text", "text": "继续比较"}])
    task = list(native.gateway.tasks.tasks.values())[-1]
    context = native.store.get("task_context:" + task["id"])
    assert {"先考虑绿色", "改成蓝色"} <= {row["content"] for row in context["messages"]}
