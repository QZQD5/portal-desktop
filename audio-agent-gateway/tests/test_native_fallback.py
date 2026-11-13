"""A stalled Being task announces its error without generating a knowledge answer."""
import pytest

from conftest import open_client

NOTICE = "模型回复流连续 60 秒没有新数据，Being 已中断本轮回复。"


@pytest.mark.parametrize("legacy_fallback", [False, True])
async def test_stalled_task_announces_only_error_and_remains_failed(running, monkeypatch, legacy_fallback):
    gateway, _, _, port = running

    async def fail(task, *args, **kwargs):
        if legacy_fallback:
            task["fallbackText"] = "旧版本生成的知识补充，不应再次播报。"
            task["fallbackSource"] = "doubao-seeduplex-knowledge"
        raise RuntimeError(NOTICE)

    monkeypatch.setattr(gateway.tasks.client, "run", fail)
    client = await open_client(port)
    try:
        task = await gateway.tasks.submit("介绍《礼运》中的大同", "test-session")
        await client.wait("task.failed")
        final = await client.wait("transcript.final", lambda e: e.get("origin") == "announcement" and task["id"] in e.get("taskIds", []))
        await client.wait("audio.done", lambda e: e["responseId"] == final["responseId"])
        await client.wait("task.notification.delivered", lambda e: e["task"]["id"] == task["id"])
        assert task["status"] == "failed" and not task.get("result")
        assert final["content"] == NOTICE
        rows = [r for r in gateway.history.page() if r.get("taskId") == task["id"]]
        assert len(rows) == 1 and rows[0]["content"] == NOTICE
        replies = [e for e in client.events if e["type"] == "transcript.final" and task["id"] in e.get("taskIds", [])]
        assert len(replies) == 1
    finally:
        await client.ws.close()
