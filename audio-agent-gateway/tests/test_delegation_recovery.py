import asyncio

import pytest

from test_voice_requests import call, native, speech  # noqa: F401


@pytest.mark.parametrize("text, answer", [
    ("帮我查一下南京明天的天气。", "好，我查一下。"),
    ("记住，你以后叫小明。", "明白，我来处理。"),
    ("查询我的长期偏好。", "我帮你看一下。"),
    ("现在有哪些任务？", "我先看看。"),
    ("取消任务。", "我这就帮你取消。"),
])
async def test_speech_and_model_text_do_not_trigger_tools(native, text, answer):
    native.settings.backend_enabled = True
    task = await native.gateway.tasks.submit("原有的工作", native.id)
    await speech(native, "user", text)
    await native.provider_event({"type": "response.created", "response": {"id": "reply"}})
    await native.provider_event({"type": "response.text.done", "response_id": "reply", "text": answer})
    await native.provider_event({"type": "response.done", "response_id": "reply", "response": {"status": "completed"}})
    assert list(native.gateway.tasks.tasks) == [task["id"]]
    assert task["status"] == "queued"
    assert not native.responses["reply"]["calls"]
    assert native.store.history(native.id)[-1]["content"] == answer
    assert native.queue.empty()


async def test_gateway_executes_the_called_tool_without_switching_it(native):
    original = await native.gateway.tasks.submit("已有工作", native.id)
    await speech(native, "user", "取消任务。")
    await call(native, "model-reply", "spawn_thinking", {"objective": "模型改写"})
    await asyncio.gather(*native.tool_tasks)
    receipt = native.responses["model-reply"]["calls"]["model-reply-call"]
    assert original["status"] == "queued"
    assert native.gateway.tasks.tasks[receipt["task_id"]]["objective"] == "模型改写"
