import asyncio
import json

import pytest
from conftest import open_client
from websockets.exceptions import ConnectionClosed

from gateway.store import now_ms
from gateway.tools import fetch_public




async def test_cancel_unresponsive_backend_and_run_next_task(running):
    gateway, _, _, port = running
    client = await open_client(port)
    try:
        await client.send("task.create", message={"parts": [{"type": "text", "text": "hang"}]})
        task = (await client.wait("task.create.result"))["task"]
        async with asyncio.timeout(5):
            while not gateway.tasks.tasks[task["id"]].get("being", {}).get("stream_id"):
                await asyncio.sleep(0.01)
        await client.send("task.cancel", task_id=task["id"])
        await client.wait("task.cancelled")
        await client.send("task.create", message={"parts": [{"type": "text", "text": "next"}]})
        assert (await client.wait("task.completed"))["task"]["result"] == "Being result: next"
    finally:
        await client.ws.close()


async def test_reminder_fires_while_backend_is_busy(running):
    gateway, _, _, port = running
    client = await open_client(port)
    try:
        await client.send("task.create", message={"parts": [{"type": "text", "text": "slow task"}]})
        await client.wait("task.running")
        reminder = await gateway.tasks.submit(
            "water",
            client.ws.request.path,
            schedule={"type": "reminder", "recurrence": "once", "executeAt": now_ms() + 50},
        )
        event = await client.wait("task.completed", timeout=1)
        assert event["task"]["id"] == reminder["id"]
        assert gateway.tasks.active is not None
    finally:
        await client.ws.close()






async def test_missing_voice_cancel_ack_reconnects_and_continues(running):
    _, speech, _, port = running
    speech.ignore_cancel = True
    client = await open_client(port)
    try:
        await client.speak(speech, "stall")
        await client.wait("response.started")
        await client.send("input.message", text="after stalled cancellation")
        event = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert "Being result: after stalled cancellation" in event["content"]
        assert len(speech.connections) == 2
    finally:
        await client.ws.close()


async def test_single_owner_takeover_and_replay(running):
    _, _, _, port = running
    first = await open_client(port)
    try:
        await first.send("input.message", text="history")
        await first.wait("audio.done")
        second = await open_client(port, instance="new-client", takeover=True)
        try:
            with pytest.raises(ConnectionClosed):
                while True:
                    await asyncio.wait_for(first.ws.recv(), 2)
            await second.send("session.replay", after_sequence=0)
            events = (await second.wait("session.replay.result"))["events"]
            assert any(e.get("content") == "history" for e in events)
            assert not any(e["type"].startswith("audio.") for e in events)
            await second.send("input.message", text="new owner")
            assert (await second.wait("transcript.final", lambda e: e["role"] == "assistant"))[
                "content"
            ] == "Being result: new owner"
        finally:
            await second.ws.close()
    finally:
        await first.ws.close()


async def test_muted_task_announcement_delivered_without_audio(running):
    _, _, _, port = running
    client = await open_client(port, output_enabled=False)
    try:
        await client.send("task.create", message={"parts": [{"type": "text", "text": "task"}]})
        await client.wait("task.notification.delivered")
        assert not any(e["type"] == "audio.delta" for e in client.events)
    finally:
        await client.ws.close()


async def test_http_task_and_sse_shapes(running):
    gateway, _, http, _ = running
    task = await gateway.tasks.submit("slow task", "http-session")
    assert (await http.get(f"/api/tasks/{task['id']}")).json()["id"] == task["id"]
    async with http.stream("GET", f"/api/tasks/{task['id']}/events") as response:
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                assert json.loads(line[6:])["type"] == "task.snapshot"
                break
    cancelled = await http.delete(f"/api/tasks/{task['id']}")
    assert cancelled.json()["status"] == "cancelled"


@pytest.mark.parametrize(
    "url", ["http://127.0.0.1/", "http://[::1]/", "file:///etc/passwd", "http://user:pass@example.com/"]
)
def test_fetch_blocks_private_targets(url):
    with pytest.raises(ValueError):
        fetch_public(url)
