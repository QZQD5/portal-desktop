import asyncio

import pytest
from test_seeduplex import native as native
from test_voice_requests import call, speech

from gateway.voice import Reply


@pytest.mark.parametrize("interruption_source", ["provider", "microphone", "client"])
async def test_interrupting_playback_leaves_future_task_result_available(native, interruption_source):
    await speech(native, "request", "查一下私信")
    await call(native, "accepted", "spawn_thinking", {"objective": "查一下私信"})
    await asyncio.gather(*native.tool_tasks)
    await native.provider_event({"type": "response.done", "response_id": "accepted"})
    tasks = native.gateway.tasks
    task = next(iter(tasks.tasks.values()))
    task["status"] = "running"
    other = await tasks.submit("另一项工作", native.id)

    reply = Reply(native.generation, origin="progress", task_id=task["id"], speech_text="正在核对私信")
    native.replies[reply.request_id] = reply
    await native.provider_event({"type": "response.created", "response": {
        "id": "progress", "metadata": {"audio_agent_gateway_request_id": reply.request_id},
    }})
    await native.playback_started("progress")
    if interruption_source == "microphone":
        await native.stop_playback_on_speech()
        native.emit.assert_any_await({"type": "playback.clear", "reason": "user_interruption"})
        await asyncio.gather(*native.background)
    elif interruption_source == "client":
        await native.playback_ended("progress", cancelled=True, reason="user_interruption")
    else:
        await speech(native, "interruption", "我还有个问题", final=False)
    assert native.responses["progress"]["playback_status"] == "interrupted"
    assert not task.get("announcements_suppressed")
    assert not task.get("discard_output")
    assert task["status"] == "running"
    assert not other.get("announcements_suppressed")
    assert not any(event.args[0].get("responseId") == "tool:accepted-call" for event in native.emit.call_args_list)

    task.update(status="completed", result="没有新私信", notificationStatus="pending")
    await tasks.publish(task)
    other.update(status="completed", result="另一项任务完成", notificationStatus="pending")
    await tasks.publish(other)
    assert task["result"] == "没有新私信"
    assert task["notificationStatus"] == "pending"

    native.settings.announcement_batch_window = 0
    await native.queue_announcements()
    announcement = native.queue.get_nowait()
    native.queue.task_done()
    assert announcement.delivery_tasks == [task["id"]]
    assert other["notificationStatus"] == "pending"


@pytest.mark.parametrize("text", ["好了", "可以了"])
async def test_speech_without_task_playback_does_not_mute_tasks(native, text):
    task = await native.gateway.tasks.submit("后台任务", native.id)
    await speech(native, "agree", text)
    assert not task.get("announcements_suppressed")
