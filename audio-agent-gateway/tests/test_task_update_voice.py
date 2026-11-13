import asyncio

from test_voice_requests import native, speech, call  # noqa: F401


async def test_voice_correction_updates_existing_work_and_suppresses_new_delegation(native):
    original = "查询桌面 CSV 文件数量和行列数"
    correction = "不是桌面目录，是本地磁盘 D"
    task = await native.gateway.tasks.submit(original, native.id, "original-turn")
    await speech(native, "correction", correction)
    await call(native, "correction-reply", "update_agent_task", {"objective": "查找 D 盘 CSV 并统计文件数量和每个文件行列数"})
    await asyncio.gather(*native.tool_tasks)
    info = native.responses["correction-reply"]
    result = info["calls"]["correction-reply-call"]
    assert result["status"] == "updated"
    assert len(native.gateway.tasks.tasks) == 1 and task["id"] == result["task_id"]
    heard = "查找 D 盘 CSV 并统计文件数量和每个文件行列数"
    assert task["objective"] == task["lastUpdate"]["userText"] == heard
    assert task["priorObjectives"] == [original]
    assert "user_message" in result
    assert not native.tool_tasks and len(native.gateway.tasks.tasks) == 1
