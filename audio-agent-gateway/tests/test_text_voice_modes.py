import asyncio

import pytest
from conftest import open_client


@pytest.mark.parametrize("running", [{"doubao_api_key": ""}], indirect=True)
async def test_text_only_uses_agent_without_voice_service_and_recovers_history(running):
    gateway, provider, _, port = running
    client = await open_client(port, text_only=True)
    try:
        for text in ("first question", "follow up"):
            await client.send("input.message", text=text)
            accepted = (await client.wait("task.accepted"))["task"]
            final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
            assert final["content"] == "Being result: " + text
            assert final["messageId"] == "agent:" + accepted["id"]
            assert final["turnId"] == accepted["turnId"]
            await client.wait("task.notification.delivered")
        assert not provider.connections
        assert not gateway.active.provider_requested.is_set()
        assert not any(e["type"] in {"audio.delta", "voice.ready", "error"} for e in client.events)
        history = gateway.store.history("test-session")
        assert [m["content"] for m in history] == [
            "first question",
            "Being result: first question",
            "follow up",
            "Being result: follow up",
        ]
        assert history[0]["turnId"] == history[1]["turnId"]
    finally:
        await client.ws.close()
    restored = await open_client(port, text_only=True)
    try:
        await restored.send("conversation.history")
        assert (await restored.wait("conversation.history.result"))["messages"] == history
        assert not provider.connections
    finally:
        await restored.ws.close()


async def test_call_text_uses_agent_and_speaks_result_then_returns_to_text(running):
    gateway, provider, _, port = running
    client = await open_client(port, text_only=True, output_enabled=False)
    try:
        await client.send("unmute")
        await client.wait("voice.ready")
        await client.send("input.message", text="answer during call")
        task = (await client.wait("task.completed"))["task"]
        final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert final["content"] == task["result"] == "Being result: answer during call"
        commits = [e["text"] for e in provider.received if e["type"] == "speech_text_buffer.commit"]
        assert commits == [task["result"]]
        await client.wait("audio.done")
        await client.wait("task.notification.delivered")
        agent_session = gateway.settings.being_scene["scene_id"]
        voice_connections = len(provider.connections)
        assert (
            len([e for e in client.events if e["type"] == "transcript.final" and e["role"] == "assistant"])
            == 1
        )
        await client.send("mute")
        await client.wait("voice.connection", lambda e: e["state"] == "disconnected")
        await client.send("input.message", text="answer after call")
        final = await client.wait("transcript.final", lambda e: e.get("origin") == "agent")
        assert final["content"] == "Being result: answer after call"
        assert gateway.settings.being_scene["scene_id"] == agent_session
        assert gateway.active.id == "test-session"
        assert len(provider.connections) == voice_connections
        assert len([e for e in provider.received if e["type"] == "speech_text_buffer.commit"]) == 1
    finally:
        await client.ws.close()


async def test_voice_remains_available_while_typed_agent_task_runs(running):
    gateway, provider, _, port = running
    client = await open_client(port)
    try:
        await client.send("input.message", text="slow work")
        task = (await client.wait("task.running"))["task"]
        await client.speak(provider, "quick voice question")
        final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert gateway.active.manual_generation is None
        assert final["content"] == "Reply: quick voice question"
        assert gateway.tasks.tasks[task["id"]]["status"] == "running"
        audio = await client.wait("audio.done")
        await client.send("playback.ended", responseId=audio["responseId"])
        completed = (await client.wait("task.completed"))["task"]
        assert completed["id"] == task["id"] and completed["status"] == "completed"
    finally:
        await client.ws.close()


async def test_text_attachment_reaches_agent_without_voice(running, monkeypatch):
    gateway, provider, _, port = running
    prompts = []
    run = gateway.tasks.client.run

    async def capture(task, context, save, progress, output=None):
        prompts.append(task["parts"])
        return await run(task, context, save, progress, output)

    monkeypatch.setattr(gateway.tasks.client, "run", capture)
    client = await open_client(port, text_only=True)
    try:
        await client.send(
            "input.message",
            parts=[
                {"type": "text", "text": "read attachment"},
                {
                    "type": "file",
                    "filename": "note.txt",
                    "mime": "text/plain",
                    "url": "data:text/plain;base64,aGVsbG8=",
                },
            ],
        )
        task = (await client.wait("task.completed"))["task"]
        assert task["parts"][0]["filename"] == "note.txt"
        assert prompts[0][0]["url"] == "data:text/plain;base64,aGVsbG8="
        assert not provider.connections
        assert gateway.active.queue.empty()
    finally:
        await client.ws.close()


async def test_ending_call_keeps_typed_task_running_and_displays_text(running):
    gateway, provider, _, port = running
    client = await open_client(port)
    try:
        await client.send("input.message", text="slow work")
        task = (await client.wait("task.running"))["task"]
        await client.send("mute")
        await client.wait("voice.connection", lambda e: e["state"] == "disconnected")
        final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert final["content"] == "Being result: slow work"
        assert final["taskIds"] == [task["id"]]
        assert gateway.tasks.tasks[task["id"]]["status"] == "completed"
        assert not any(e["type"] == "speech_text_buffer.commit" for e in provider.received)
    finally:
        await client.ws.close()


@pytest.mark.parametrize("end_call", [False, True])
async def test_ending_call_after_asr_cancels_unsubmitted_voice_request(running, end_call):
    gateway, provider, _, port = running
    provider.voice_delay = 0.3
    client = await open_client(port)
    try:
        await client.speak(provider, "查下今天天气。")
        await client.wait("transcript.final", lambda e: e["role"] == "user")
        assert gateway.active.native_pending_since
        if end_call:
            await client.send("mute")
            await client.wait("voice.connection", lambda e: e["state"] == "disconnected")
            await asyncio.sleep(0.4)
            assert not gateway.active.native_pending_since
            assert not gateway.active.provider_requested.is_set()
            assert not any(e["type"] == "response.started" for e in client.events)
            assert [m["role"] for m in gateway.store.history("test-session")] == ["user"]
        else:
            reply = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
            assert reply["content"] == "Reply: 查下今天天气。"
        assert not gateway.tasks.tasks
    finally:
        await client.ws.close()




@pytest.mark.parametrize("text", ["查下今天天气。", "delegate weather"])
async def test_input_mute_preserves_pending_voice_and_tool_calls(running, text):
    gateway, provider, _, port = running
    provider.voice_delay = 0.3
    client = await open_client(port)
    try:
        await client.speak(provider, text)
        await client.wait("transcript.final", lambda e: e["role"] == "user")
        session = gateway.active
        generation = session.generation
        await client.send("input.mute")
        async with asyncio.timeout(2):
            while session.input_enabled:
                await asyncio.sleep(0.01)
        assert session.native_pending_since and session.generation == generation
        appended = sum(e["type"] == "input_audio_buffer.append" for e in provider.received)
        await session.append_audio("AAAA")
        assert sum(e["type"] == "input_audio_buffer.append" for e in provider.received) == appended
        if "delegate" in text:
            await client.wait("task.accepted")
        reply = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert reply["content"] == ("tool completed" if "delegate" in text else "Reply: " + text)
        await client.wait("audio.done")
        assert session.ready.is_set() and session.output_enabled
        assert len(provider.connections) == 1
        assert not any(e["type"] in {"response.cancel", "session.close"} for e in provider.received)
        await client.send("input.unmute")
        async with asyncio.timeout(2):
            while not session.input_enabled:
                await asyncio.sleep(0.01)
        assert session.generation == generation
    finally:
        await client.ws.close()
