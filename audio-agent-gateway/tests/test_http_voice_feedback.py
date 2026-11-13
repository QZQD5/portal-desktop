import asyncio
import json

import httpx
import pytest

from conftest import open_client
from test_conversation_context import read_context, voice_updates
from test_local_chat import event
from test_callback_chat import callback


async def test_http_only_spoken_result_is_shared_with_next_turn(running, monkeypatch):
    gateway, provider, http, port = running
    release = asyncio.Event()
    posted = []
    send = gateway.tasks.client.http.send

    class Work(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield event("meta", {"stream_id": "being-text", "scene_id": "fixture-scene"})
            yield event("tool_use", {"name": "fixture"})
            await release.wait()
            yield event("content_block_delta", {"delta": {"text": "实际结果：选择绿色。"}})
            yield event("message_stop", {})

    async def capture(request, **kwargs):
        if request.method == "POST" and request.url.path.endswith("/chat/stream"):
            posted.append(json.loads(request.content))
            if len(posted) == 1:
                return httpx.Response(200, request=request, headers={"content-type": "text/event-stream"}, stream=Work())
        return await send(request, **kwargs)

    monkeypatch.setattr(gateway.tasks.client.http, "send", capture)
    client = await open_client(port)
    posting = asyncio.create_task(http.post("/api/heart/chat/stream", json={
        "message": "帮我选一个颜色", "scene_id": "fixture-scene", "client_request_id": "with-progress",
    }))
    try:
        await client.wait("task.activity")
        assert not posting.done()
        assert next(iter(gateway.tasks.tasks.values()))["status"] == "running"
        assert not any(e["type"] == "speech_text_buffer.commit" for e in provider.received)
        release.set()
        assert (await posting).status_code == 200
        final = await client.wait("transcript.final", lambda e: e.get("origin") == "text" and e["content"] == "实际结果：选择绿色。")
        await client.wait("audio.done", lambda e: e["responseId"] == final["responseId"])
        assert any(e["type"] == "speech_text_buffer.commit" and e["text"] == final["content"] for e in provider.received)
        assert [row["content"] for row in gateway.history.page()] == ["帮我选一个颜色", "实际结果：选择绿色。"]
        assert gateway.active.context_sent["textWork"] is None
        assert any(row["content"] == final["content"] for update in voice_updates(provider) for row in update["messages"])
        await client.speak(provider, "再考虑蓝色")
        await client.wait("transcript.final", lambda e: e.get("role") == "assistant" and "再考虑蓝色" in e["content"])
        await http.post("/api/heart/chat/stream", json={"message": "综合比较", "scene_id": "fixture-scene"})
        context = read_context(posted[-1]["message"])
        assert {row["content"] for row in context["messages"]} == {"再考虑蓝色", "Reply: 再考虑蓝色"}
        await provider.connections[-1].close()
        await client.wait("voice.ready")
        assert any(row["content"] == "实际结果：选择绿色。" for row in gateway.active.context_sent["messages"])
    finally:
        release.set()
        await asyncio.gather(posting, return_exceptions=True)
        await client.ws.close()


async def test_text_without_open_voice_retains_only_its_scene_context(running, monkeypatch):
    gateway, provider, http, _ = running
    gateway.store.message("earlier", "user", "语音讨论过绿色", id="spoken")
    gateway.history.put("other", {"role": "user", "content": "另一场景的私有内容", "scene_id": "other"})
    posted = []
    send = gateway.tasks.client.http.send

    async def capture(request, **kwargs):
        if request.method == "POST":
            posted.append(json.loads(request.content))
        return await send(request, **kwargs)

    monkeypatch.setattr(gateway.tasks.client.http, "send", capture)
    response = await http.post("/api/heart/chat/stream", json={"message": "继续比较", "scene_id": "fixture-scene"})
    assert response.status_code == 200
    assert [row["content"] for row in read_context(posted[0]["message"])["messages"]] == ["语音讨论过绿色"]
    await http.post("/api/heart/chat/stream", json={"message": "另一个问题", "scene_id": "other"})
    assert [row["content"] for row in read_context(posted[1]["message"])["messages"]] == ["另一场景的私有内容"]
    assert not provider.connections


@pytest.mark.parametrize("failure", ["rejected", "unknown"])
async def test_http_failure_is_spoken_without_claiming_completion(running, monkeypatch, failure):
    gateway, _, http, port = running
    send = gateway.tasks.client.http.send
    async def fail(request, **kwargs):
        if request.method != "POST":
            return await send(request, **kwargs)
        if failure == "unknown":
            raise httpx.ReadError("fixture disconnected")
        return httpx.Response(503, request=request, json={"error": "unavailable"})
    monkeypatch.setattr(gateway.tasks.client.http, "send", fail)
    client = await open_client(port)
    try:
        response = await http.post("/api/heart/chat/stream", json={"message": "测试失败", "scene_id": "fixture-scene"})
        assert response.status_code == 200 and "event: error" in response.text
        spoken = await client.wait("transcript.final", lambda e: e.get("origin") == "text")
        assert ("未取得流标识" if failure == "unknown" else "HTTP 503") in spoken["content"]
        assert next(iter(gateway.tasks.tasks.values()))["status"] == "failed"
    finally:
        await client.ws.close()


async def test_recovered_text_result_is_spoken_once_without_resubmitting(running, monkeypatch):
    gateway, provider, http, port = running
    posts = []
    class Interrupted(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield event("meta", {"stream_id": "recover"})
            yield event("content_block_delta", {"delta": {"text": "前半"}})
            raise httpx.ReadError("fixture disconnected")
    async def respond(request, **_):
        if request.method == "POST":
            posts.append(request)
            return httpx.Response(200, request=request, headers={"content-type": "text/event-stream"}, stream=Interrupted())
        if not posts:
            return httpx.Response(204, request=request)
        assert request.url.params["after"] == "0"
        return httpx.Response(200, request=request, json={"stream_id": "recover", "finished": True, "events": [
            {"seq": 1, "event": "content_block_delta", "data": {"delta": {"text": "前半"}}},
            {"seq": 2, "event": "content_block_delta", "data": {"delta": {"text": "后半"}}},
            {"seq": 3, "event": "message_stop", "data": {}},
        ]})
    monkeypatch.setattr(gateway.tasks.client.http, "send", respond)
    client = await open_client(port)
    try:
        response = await http.post("/api/heart/chat/stream", json={"message": "恢复结果", "scene_id": "fixture-scene"})
        assert response.status_code == 200 and len(posts) == 1
        final = await client.wait("transcript.final", lambda e: e.get("origin") == "text" and e["content"] == "前半后半")
        await client.wait("audio.done", lambda e: e["responseId"] == final["responseId"])
        assert [e["text"] for e in provider.received if e["type"] == "speech_text_buffer.commit"] == ["前半后半"]
        assert [row["content"] for row in gateway.history.page()] == ["恢复结果", "前半后半"]
    finally:
        await client.ws.close()


async def test_global_portal_callback_is_spoken_once_and_retained_in_voice_context(running, monkeypatch):
    gateway, provider, _, port = running
    original = gateway.tasks.client.http.send
    posted = []

    async def respond(request, **kwargs):
        if request.url.path.endswith("/api/stream/active"):
            return httpx.Response(200, request=request, json=callback(text="后台操作已经完成。"))
        if request.method == "POST":
            posted.append(request)
        return await original(request, **kwargs)

    monkeypatch.setattr(gateway.tasks.client.http, "send", respond)
    client = await open_client(port)
    try:
        await gateway.chat.discover_callback()
        await gateway.chat.job
        final = await client.wait("transcript.final", lambda e: e.get("origin") == "text"
                                  and e["content"] == "后台操作已经完成。")
        await client.wait("audio.done", lambda e: e["responseId"] == final["responseId"])
        await gateway.chat.discover_callback()
        assert [e["text"] for e in provider.received if e["type"] == "speech_text_buffer.commit"] == ["后台操作已经完成。"]
        assert any(row["content"] == "后台操作已经完成。" for row in gateway.active.context_sent["messages"])
        assert [row["content"] for row in gateway.history.page()] == ["后台操作已经完成。"]
        assert not posted
    finally:
        await client.ws.close()
