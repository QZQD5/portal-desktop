import asyncio
import json
from unittest.mock import AsyncMock

from conftest import open_client
from test_conversation_context import read_context, voice_updates


async def test_http_text_interrupts_voice_and_queues_without_cancelling_being_task(running, monkeypatch):
    gateway, provider, http, port = running
    started, release = asyncio.Event(), asyncio.Event()
    run = gateway.tasks.client.run
    async def background(task, *args):
        if task["objective"] != "background work":
            return await run(task, *args)
        async with gateway.tasks.client.turn_lock:
            started.set()
            await release.wait()
            return "background completed"
    monkeypatch.setattr(gateway.tasks.client, "run", background)
    cancel = AsyncMock(side_effect=AssertionError("Text must not cancel a Being task"))
    monkeypatch.setattr(gateway.tasks.client, "cancel", cancel)
    client = await open_client(port)
    posting = None
    try:
        task = await gateway.tasks.submit("background work", "test-session")
        await asyncio.wait_for(started.wait(), 2)
        await client.speak(provider, "stall")
        old = (await client.wait("response.started"))["responseId"]
        session = gateway.active
        await session.provider_event({"type": "response.audio.delta", "response_id": old, "delta": "AAAAAA=="})
        await client.wait("audio.delta", lambda e: e["responseId"] == old)
        body = {"message": "改用文字", "scene_id": "fixture-scene", "client_request_id": "typed-once"}
        posting = asyncio.create_task(http.post("/api/heart/chat/stream", json=body))
        await client.wait("playback.clear", lambda e: e.get("reason") == "text_input")
        assert session.responses[old]["suppressed"]
        assert not session.playing and task["status"] == "running"
        async with asyncio.timeout(2):
            while not gateway.history.state("request:typed-once"):
                await asyncio.sleep(.01)
        assert session.manual_generation is None
        generation = session.generation
        async with asyncio.timeout(2):
            while not any(e["type"] == "response.cancel" for e in provider.received):
                await asyncio.sleep(.01)
        assert (await http.post("/api/heart/chat/stream", json=body)).status_code == 202
        assert session.generation == generation  # Retrying cannot interrupt a later voice turn.
        typed = await client.wait("task.accepted", lambda event: event["task"]["objective"] == "改用文字")
        assert typed["task"]["status"] == "queued" and task["status"] == "running"
        assert not posting.done()
        release.set()
        response = await asyncio.wait_for(posting, 2)
        assert response.status_code == 200 and "Being result: 改用文字" in response.text
        assert task["status"] == "completed"
        cancel.assert_not_called()
        boundary = next(i for i, e in enumerate(client.events) if e["type"] == "playback.clear" and e.get("reason") == "text_input")
        await client.send("conversation.history")
        await client.wait("conversation.history.result")
        assert not any(e["type"] == "audio.delta" and e["responseId"] == old for e in client.events[boundary + 1:])
    finally:
        release.set()
        if posting:
            await asyncio.gather(posting, return_exceptions=True)
        await client.ws.close()


async def test_http_text_syncs_both_contexts_while_voice_and_being_work_in_parallel(running, monkeypatch):
    gateway, provider, http, port = running
    started, release = asyncio.Event(), asyncio.Event()
    text_posted, text_headers = asyncio.Event(), asyncio.Event()
    run = gateway.tasks.client.run
    async def background(task, *args):
        if task["objective"] != "background work":
            return await run(task, *args)
        async with gateway.tasks.client.turn_lock:
            started.set()
            await release.wait()
        return "后台的真实结果"
    monkeypatch.setattr(gateway.tasks.client, "run", background)
    posted = []
    send = gateway.tasks.client.http.send
    async def capture(request, **kwargs):
        if request.method == "POST" and request.url.path.endswith("/chat/stream"):
            posted.append(json.loads(request.content))
            text_posted.set()
            await text_headers.wait()
        return await send(request, **kwargs)
    monkeypatch.setattr(gateway.tasks.client.http, "send", capture)
    (gateway.settings.data_dir / "USER.md").write_text("Prefer option two.", encoding="utf-8")
    client = await open_client(port)
    posting = None
    try:
        task = await gateway.tasks.submit("background work", "test-session")
        await asyncio.wait_for(started.wait(), 2)
        await client.speak(provider, "options: red green blue")
        await client.wait("audio.done")
        async with asyncio.timeout(2):
            while not any(row.get("playbackStatus") == "completed" for row in gateway.history.page()):
                await asyncio.sleep(.01)
        posting = asyncio.create_task(http.post("/api/heart/chat/stream", json={
            "message": "use option two", "scene_id": "fixture-scene", "client_request_id": "http-context",
        }))
        typed = await client.wait("task.accepted", lambda event: event["task"]["objective"] == "use option two")
        async with asyncio.timeout(2):
            while not any(row["content"] == "use option two"
                          for update in voice_updates(provider) for row in update["messages"]):
                await asyncio.sleep(.01)
        assert not text_posted.is_set() and task["status"] == "running"
        release.set()
        await asyncio.wait_for(text_posted.wait(), 2)
        text_headers.set()
        response = await asyncio.wait_for(posting, 2)
        assert response.status_code == 200 and task["status"] == "completed"
        assert len(posted) == 1
        context = read_context(posted[0]["message"])
        assert any("red green blue" in row["content"] for row in context["messages"])
        assert any(row.get("playbackStatus") == "completed" for row in context["messages"])
        assert context["memory"] == []
        assert context["tasks"][0]["id"] == task["id"] and context["tasks"][0]["status"] == "running"
        assert context["currentRequest"]["taskId"] == typed["task"]["id"]
        assert len(gateway.tasks.tasks) == 2
        async with asyncio.timeout(2):
            while not any(row.get("taskId") == typed["task"]["id"] for update in voice_updates(provider) for row in update["messages"]):
                await asyncio.sleep(.01)
        assert any(row["content"] == "use option two" and row.get("source") == "text-user"
                   for update in voice_updates(provider) for row in update["messages"])
        await client.wait("transcript.final", lambda e: e.get("origin") == "text" and e["content"] == "Being result: use option two")
        assert any(e["type"] == "speech_text_buffer.commit" and e["text"] == "Being result: use option two" for e in provider.received)
        await provider.connections[-1].close()
        await client.wait("voice.ready")
        restored = voice_updates(provider)[-1]
        assert restored["snapshot"] and any(row.get("taskId") == typed["task"]["id"] for row in restored["messages"])
        await client.speak(provider, "continue chatting")
        await client.wait("transcript.final", lambda e: e.get("role") == "assistant" and "continue chatting" in e.get("content", ""))
        assert task["status"] == "completed"
        async with asyncio.timeout(2):
            while not any(row.get("taskId") == task["id"] and row["content"] == "后台的真实结果"
                          for update in voice_updates(provider) for row in update["messages"]):
                await asyncio.sleep(.01)
    finally:
        release.set()
        text_headers.set()
        if posting:
            await asyncio.gather(posting, return_exceptions=True)
        await client.ws.close()


async def test_invalid_or_other_scene_text_does_not_interrupt_voice(running):
    gateway, provider, http, port = running
    client = await open_client(port)
    try:
        await client.speak(provider, "stall")
        old = (await client.wait("response.started"))["responseId"]
        session, generation = gateway.active, gateway.active.generation
        assert (await http.post("/api/heart/chat/stream", json={"message": None, "scene_id": "fixture-scene"})).status_code == 400
        response = await http.post("/api/heart/chat/stream", json={"message": "另一场景的文字", "scene_id": "other"})
        assert response.status_code == 200
        assert session.generation == generation and not session.responses[old]["suppressed"]
        assert not any(e["type"] == "response.cancel" for e in provider.received)
    finally:
        await client.ws.close()


async def test_simultaneous_text_preserves_final_asr_context_and_blocks_late_native_replies(running, monkeypatch):
    gateway, _, http, port = running
    client = await open_client(port)
    try:
        session = gateway.active
        await session.provider_input({"type": "input_audio_buffer.speech_started", "item_id": "old-asr"})
        response = await http.post("/api/heart/chat/stream", json={"message": "以这条文字为准", "scene_id": "fixture-scene"})
        assert response.status_code == 200
        assert session.input_records["old-asr"]["invalid"]
        await session.provider_input({"type": "conversation.item.input_audio_transcription.completed",
                                      "item_id": "old-asr", "transcript": "迟到的识别"})
        await session.provider_event({"type": "response.created", "response": {"id": "late-reply"}})
        assert session.responses["late-reply"]["suppressed"]
        assert any(row["content"] == "迟到的识别" for row in gateway.history.page())
        assert any(row["content"] == "迟到的识别" for row in session.context_sent["messages"])
        posted = []
        send = gateway.tasks.client.http.send
        async def capture(request, **kwargs):
            if request.method == "POST":
                posted.append(json.loads(request.content))
            return await send(request, **kwargs)
        monkeypatch.setattr(gateway.tasks.client.http, "send", capture)
        await http.post("/api/heart/chat/stream", json={"message": "结合刚才的语音", "scene_id": "fixture-scene"})
        assert any(row["content"] == "迟到的识别" for row in read_context(posted[0]["message"])["messages"])
    finally:
        await client.ws.close()
