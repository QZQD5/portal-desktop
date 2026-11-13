import asyncio
import json

import httpx
import pytest

from gateway.being import BeingClient
from gateway.store import Store
from gateway.tasks import TaskManager
from gateway.subagent_tasks import observe, pending
from test_being import settings, event, Chunks


def receipt(task_id="sub_child", status="running"):
    return {"content": [{"type": "text", "text": json.dumps({"task_id": task_id, "status": status})}]}


@pytest.mark.parametrize("value", [receipt(), json.dumps(receipt()), receipt()["content"]])
def test_only_correlated_spawn_receipts_create_children(value):
    state = {}
    observe(state, "tool_result", {"id": "unrelated", "content": receipt("sub_other")})
    assert not pending(state)
    observe(state, "tool_use", {"id": "spawn", "name": "portal_subagent_spawn"})
    assert pending(state)
    observe(state, "tool_result", {"id": "spawn", "content": value})
    assert state["subagents"] == {"sub_child": "running"} and pending(state)
    observe(state, "tool_use", {"id": "status", "name": "portal_subagent_status"})
    observe(state, "tool_result", {"id": "status", "content": receipt(status="done")})
    assert not pending(state)


def test_unknown_receipts_remain_pending_and_explicit_errors_do_not_create_children():
    state = {}
    observe(state, "tool_use", {"id": "a", "name": "portal_subagent_spawn"})
    observe(state, "tool_result", {"id": "a", "content": '{"unexpected":true}'})
    assert pending(state)
    observe(state, "tool_result", {"id": "a", "is_error": True, "content": "failed"})
    assert not pending(state)
    observe(state, "tool_use", {"id": "b", "name": "portal_subagent_spawn"})
    observe(state, "tool_result", {"id": "b", "content": json.dumps({"isError": True, "content": [{"text": "failed"}]})})
    assert not pending(state)


def test_actual_act_wrapper_receipts_and_help_queries():
    state = {}
    observe(state, "tool_use", {"id": "help", "name": "act", "input": {"body": "portal_subagent_spawn ?"}})
    assert not pending(state)


    observe(state, "tool_use", {"id": "spawn", "name": "act", "input": {"body": "portal_subagent_spawn session=project brief=work"}})
    assert pending(state)
    observe(state, "tool_result", {"id": "spawn", "summary": "short display summary", "content":
                                    '[local-portal] [act ✓ 0step completed] ' + json.dumps({"task_id": "sub_child", "status": "running"})})
    assert state["subagents"] == {"sub_child": "running"}
    observe(state, "tool_use", {"id": "status", "name": "act", "input": json.dumps({"body": "portal_subagent_status task_id=sub_child"})})
    observe(state, "tool_result", {"id": "status", "content": '[local-portal] [act ✓ 0step completed] ' + json.dumps({"task": {"task_id": "sub_child", "status": "done"}})})
    assert not pending(state)
    observe(state, "tool_use", {"id": "failed", "name": "act", "input": {"body": "portal_subagent_spawn brief=work"}})
    observe(state, "tool_result", {"id": "failed", "result": {"exit_code": 1, "content": "failed"}})
    assert not pending(state)


def test_real_act_events_without_call_ids_and_with_120_character_results():
    state = {}
    child = "sub_01234567-89ab-cdef-0123-456789abcdef"
    prefix = '[portal-desktop-D5-NJ-DT-0273] [act ✓ 0step completed] '
    observe(state, "tool_use", {"name":"act", "input":{"body":"portal_subagent_spawn brief=work"}})
    summary = (prefix + json.dumps({"summary":child + ' running',"task_id":child,"status":"running"},separators=(',',':')))[:120]
    observe(state,"tool_result",{"name":"act","summary":summary,"is_error":False})
    assert state["subagents"] == {child:"running"} and pending(state)
    observe(state,"tool_use",{"name":"act","input":{"body":"portal_subagent_control action=cancel task_id="+child}})
    summary = (prefix + json.dumps({"summary":child + ' cancelled',"task_id":child,"status":"cancelled"},separators=(',',':')))[:120]
    observe(state,"tool_result",{"name":"act","summary":summary,"is_error":False})
    assert not pending(state)


def test_parallel_unidentified_act_calls_do_not_attribute_unrelated_output_to_a_spawn():
    state = {}
    observe(state, "tool_use", {"name":"act", "input":{"body":"portal_file_read path=data.txt"}})
    observe(state, "tool_use", {"name":"act", "input":{"body":"portal_subagent_spawn brief=work"}})
    observe(state, "tool_result", {"name":"act", "summary":'{"summary":"sub_unrelated done"}'})
    assert pending(state) and not state.get("subagents")


def test_unidentified_calls_do_not_overwrite_another_pending_call():
    state = {}
    for _ in range(2):
        observe(state, "tool_use", {"name": "portal_subagent_spawn"})
    observe(state, "tool_result", {"content": receipt("sub_first", "done")})
    observe(state, "tool_use", {"name": "portal_subagent_spawn"})
    observe(state, "tool_result", {"content": receipt("sub_second", "done")})
    assert pending(state)
    observe(state, "tool_result", {"content": receipt("sub_third", "done")})
    assert not pending(state) and len(state["subagents"]) == 3


async def test_delegated_voice_task_is_not_completed_or_falsely_cancelled_and_survives_restart(tmp_path):
    calls, events = [], []
    body = b"".join([event("meta", {"stream_id": "original"}),
                     event("tool_use", {"id": "spawn", "name": "portal_subagent_spawn"}),
                     event("tool_result", {"id": "spawn", "content": receipt()}),
                     event("content_block_delta", {"delta": {"text": "Delegated; will report later."}}),
                     event("message_stop", {})])

    async def respond(req):
        calls.append(req.method)
        return httpx.Response(204) if req.method == "GET" else httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=Chunks(body))

    async def emit(_, value):
        events.append(value)

    store = Store(tmp_path / "state.sqlite3")
    manager = TaskManager(settings(tmp_path), store, emit)
    await manager.client.close()
    manager.client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond), store=store)
    task = await manager.submit("work", "session")
    try:
        await manager.execute(task)
        assert task["status"] == "delegated" and task["result"] is None and task["completedAt"] is None
        assert not any(value["type"] == "task.completed" for value in events)
        with pytest.raises(ValueError, match="子任务停止"):
            await manager.cancel(task["id"])
        assert task["status"] == "delegated" and calls.count("POST") == 2
        restarted = TaskManager(settings(tmp_path), store, emit)
        try:
            assert restarted.tasks[task["id"]]["status"] == "delegated"
        finally:
            await restarted.close()
    finally:
        await manager.close()
        store.close()


@pytest.mark.parametrize("confirmed", [True, False])
async def test_cancellation_requires_actual_child_tool_receipts(tmp_path, confirmed):
    sent = []
    async def respond(req):
        sent.append(json.loads(req.content))
        chunks = [event("meta", {"stream_id": "cancel-stream"})]
        if confirmed:
            chunks += [event("tool_use", {"id": "cancel", "name": "portal_subagent_control",
                                          "input": {"action": "cancel", "task_id": "sub_child"}}),
                       event("tool_result", {"id": "cancel", "content": receipt(status="cancelled")})]
        chunks += [event("content_block_delta", {"delta": {"text": "已取消"}}), event("message_stop", {})]
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Chunks(b"".join(chunks)))

    client = BeingClient(settings(tmp_path), transport=httpx.MockTransport(respond))
    task = {"being": {"stream_finished": True, "subagents": {"sub_child": "running"}}}
    try:
        if confirmed:
            await client.cancel(task)
            assert task["being"]["subagents"] == {"sub_child": "cancelled"}
        else:
            with pytest.raises(ValueError, match="尚未确认"):
                await client.cancel(task)
            assert pending(task["being"])
        assert len(sent) == 1 and "sub_child" in sent[0]["message"]
    finally:
        await client.close()


@pytest.mark.parametrize("confirmed,cancelled", [(True, False), (False, False), (True, True)])
async def test_callback_closes_only_the_task_with_confirmed_children_without_duplicate_history(tmp_path, confirmed, cancelled):
    from gateway.chat import Chat
    from gateway.local_history import LocalHistory

    snapshot = {"stream_id": "callback-result", "origin": "callback", "scene_id": None,
                "finished": True, "events": [
                    {"seq": 1, "event": "content_block_delta", "data": {"delta": {"text": "实际完成结果"}}},
                ]}
    if confirmed:
        snapshot["events"] += [
            {"seq": 2, "event": "tool_use", "data": {"id": "status", "name": "portal_subagent_status"}},
            {"seq": 3, "event": "tool_result", "data": {"id": "status", "content": receipt(status="done")}}]
    snapshot["events"] += [
        {"seq": len(snapshot["events"]) + 1, "event": "message_stop", "data": {}}]
    store = Store(tmp_path / "store.sqlite3")
    history = LocalHistory(tmp_path / "history.sqlite3")
    store.attach_history(history, settings(tmp_path).being_scene)
    async def emit(*_):
        pass
    manager = TaskManager(settings(tmp_path), store, emit)
    await manager.client.close()
    manager.client = BeingClient(settings(tmp_path), store=store,
                                transport=httpx.MockTransport(lambda req: httpx.Response(200, json=snapshot)))
    task = await manager.submit("work", "session")
    task.update(status="cancelled" if cancelled else "delegated", discard_output=cancelled,
                being={"subagents": {"sub_child": "cancelled" if cancelled else "running"}})
    other = await manager.submit("unrelated", "session")
    other.update(status="delegated", being={"subagents": {"sub_other": "running"}})
    chat = Chat(manager.client, history, tasks=manager)
    try:
        await chat.discover_callback()
        await chat.job
        assert task["status"] == ("cancelled" if cancelled else "completed" if confirmed else "delegated")
        assert other["status"] == "delegated"
        assert [row["content"] for row in history.page()] == ([] if cancelled else ["实际完成结果"])
        assert any(item["event"] == "content_block_delta" for item in chat.events) is not cancelled
        await chat.discover_callback()
        assert len(history.page()) == (0 if cancelled else 1)
    finally:
        await chat.close()
        await manager.close()
        history.close()
        store.close()


@pytest.mark.parametrize("source", ["metadata", "marker"])
async def test_callback_discards_only_the_cancelled_reply_and_preserves_other_replies(tmp_path, source):
    from gateway.chat import Chat
    from gateway.local_history import LocalHistory

    settings_for_chat = settings(tmp_path)
    settings_for_chat.max_terminal_tasks = 0
    scene_id = settings_for_chat.being_scene["scene_id"]
    store = Store(tmp_path / "store.sqlite3")
    history = LocalHistory(tmp_path / "history.sqlite3")
    spoken = []

    async def emit(*_):
        pass

    async def feedback(_, message):
        if message and message["text"]:
            spoken.append((message["text"], message["task_ids"]))

    manager = TaskManager(settings_for_chat, store, emit)
    cancelled = await manager.submit("取消的工作", "session")
    cancelled["being"] = {"subagents": {"sub_cancelled": "done"}}
    normal = await manager.submit("其他工作", "session")
    normal.update(status="delegated", being={"subagents": {"sub_normal": "running"}})
    await manager.cancel(cancelled["id"])
    assert cancelled["id"] not in manager.tasks

    def delta(text):
        return ("content_block_delta", {"delta": {"text": text}})

    if source == "metadata":
        initial = {"task_id": "sub_cancelled"}
        messages = [
            delta("过期结果"), ("message_stop", {}),
            ("meta", {"scene_id": scene_id, "continuation": True, "task_id": "sub_normal"}),
            delta("其他任务结果"),
        ]
    else:
        initial = {}
        messages = [
            delta("<!-- gateway-"), delta(f"task: {cancelled['id']} -->\n过期结果"), ("message_stop", {}),
            delta(f"<!-- gateway-task: {normal['id']} -->\n其他任务结果"),
        ]
    messages += [
        ("tool_use", {"id": "normal-status", "name": "portal_subagent_status"}),
        ("tool_result", {"id": "normal-status", "content": receipt("sub_normal", status="done")}),
        ("message_stop", {}),
        ("meta", {"scene_id": scene_id, "continuation": True, "task_id": "sub_unrelated"}),
        delta("独立回调"), ("message_stop", {}),
    ]
    active = {"stream_id": "mixed", "origin": "callback", "scene_id": None, "finished": False, **initial,
              "events": [{"seq": seq, "event": kind, "data": data}
                         for seq, (kind, data) in enumerate(messages, 1)]}
    await manager.client.close()
    manager.client = BeingClient(settings_for_chat, transport=httpx.MockTransport(
        lambda _: httpx.Response(200, json=active),
    ))
    chat = Chat(manager.client, history, tasks=manager, context_changed=feedback)
    try:
        await chat.discover_callback()
        async with asyncio.timeout(2):
            while len(spoken) < 2:
                await asyncio.sleep(.01)
        assert not chat.current["finished"]
        active["finished"] = True
        await asyncio.wait_for(chat.job, 2)
        assert normal["status"] == "completed" and normal["result"] == "其他任务结果"
        assert [(row["scene_id"], row["content"]) for row in history.page()] == [
            (scene_id, "其他任务结果"), (scene_id, "独立回调"),
        ]
        assert spoken == [("其他任务结果", [normal["id"]]), ("独立回调", [])]
        assert [item["data"]["delta"]["text"] for item in chat.events if item["event"] == "content_block_delta"] == [
            "其他任务结果", "独立回调",
        ]
    finally:
        await chat.close()
        await manager.close()
        history.close()
        store.close()
