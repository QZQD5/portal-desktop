import asyncio
import json
from unittest.mock import AsyncMock

from conftest import open_client

from gateway.conversation_context import snapshot
from gateway.store import Store


def read_context(text):
    body = text.split("<conversation_context>\n", 1)[1].split("\n</conversation_context>", 1)[0]
    return json.loads(body.split("\n", 1)[1])


def voice_updates(provider):
    return [
        read_context(item["content"][0]["text"])
        for event in provider.received
        if event["type"] == "conversation.item.create"
        for item in event["items"]
        if item.get("role") == "user"
        and item["content"][0].get("text", "").startswith("<conversation_context>")
    ]


def capture_prompts(gateway, monkeypatch):
    contexts = []
    run = gateway.tasks.client.run

    async def capture(task, context, save, progress, output=None):
        contexts.append(read_context(context))
        return await run(task, context, save, progress, output)

    monkeypatch.setattr(gateway.tasks.client, "run", capture)
    return contexts


def test_snapshot_preserves_canonical_results_order_budget_and_scope(tmp_path):
    store = Store(tmp_path / "context.db")
    try:
        store.message("other", "user", "private conversation")
        store.message("s", "assistant", "orphan reply", id="orphan")
        store.message("s", "user", "first", id="u1")
        store.message("s", "user", "correction", id="u2")
        store.message("s", "assistant", "full result", id="result", source="agent-result", taskId="t")
        store.message(
            "s", "assistant", "short spoken result", id="spoken", source="agent-presentation", taskId="t"
        )
        assert "full result" not in [m["content"] for m in store.history("s")]
        tasks = {"foreign": {"sessionId": "other", "id": "foreign", "objective": "private task"}}
        context = snapshot(store, tasks, "s")
        assert [m["id"] for m in context["messages"]] == ["orphan", "u1", "u2", "result", "spoken"]
        assert context["tasks"] == []
        assert context == snapshot(store, tasks, "s")
        store.message("s", "assistant", "short spoken result", id="spoken", playbackStatus="interrupted")
        assert snapshot(store, tasks, "s")["version"] != context["version"]
        for i in range(8):
            store.message("s", "user", "x" * 10000, id=f"long-{i}")
        bounded = snapshot(store, tasks, "s")
        assert sum(len(m["content"]) for m in bounded["messages"]) == 20000
        assert all(m["truncated"] for m in bounded["messages"])
    finally:
        store.db.close()


async def test_voice_history_reaches_next_text_task_without_local_memory(running, monkeypatch):
    gateway, provider, _, port = running
    contexts = capture_prompts(gateway, monkeypatch)
    (gateway.settings.data_dir / "USER.md").write_text("Prefer option two.", encoding="utf-8")
    client = await open_client(port)
    try:
        await client.speak(provider, "three options: red, green, blue")
        audio = await client.wait("audio.done")
        await client.send("playback.ended", responseId=audio["responseId"])
        await client.send("input.message", text="use the second option")
        task = (await client.wait("task.completed"))["task"]
        context = contexts[0]
        assert task["contextVersion"] == context["version"]
        assert [m["content"] for m in context["messages"]] == [
            "three options: red, green, blue",
            "Reply: three options: red, green, blue",
        ]
        assert context["messages"][1]["playbackStatus"] == "completed"
        assert task["objective"] == "use the second option"
        assert "memory" not in voice_updates(provider)[0]
        assert "memory" not in context
    finally:
        await client.ws.close()


async def test_typed_call_input_is_quiet_context_and_cannot_be_delegated_twice(running):
    gateway, provider, _, port = running
    client = await open_client(port)
    try:
        await client.send("input.message", text="slow work: budget is now 500")
        task = (await client.wait("task.running"))["task"]
        async with asyncio.timeout(2):
            while not any(
                m["content"] == task["objective"]
                for update in voice_updates(provider)
                for m in update["messages"]
            ):
                await asyncio.sleep(0.01)
        assert not any(e["type"] == "speech_text_buffer.commit" for e in provider.received)
        result = await gateway.tools.execute(
            "spawn_thinking", {"objective": "reworded duplicate"}, gateway.active
        )
        assert result["status"] == "already_submitted" and result["task_id"] == task["id"]
        assert len(gateway.tasks.tasks) == 1
        final = await client.wait("task.completed")
        assert final["task"]["id"] == task["id"]
        await client.wait("audio.done")
        assert [e["text"] for e in provider.received if e["type"] == "speech_text_buffer.commit"] == [
            final["task"]["result"]
        ]
    finally:
        await client.ws.close()


async def test_context_cursor_deduplicates_updates_and_ignores_local_memory(running, monkeypatch):
    gateway, _, _, port = running
    client = await open_client(port)
    try:
        voice = gateway.active
        send = AsyncMock()
        monkeypatch.setattr(voice, "send_item", send)
        await voice.sync_context()
        send.assert_not_awaited()
        (gateway.settings.data_dir / "MEMORY.md").write_text("new fact", encoding="utf-8")
        await voice.sync_context()
        send.assert_not_awaited()
        gateway.store.message(voice.id, "user", "new message")
        await voice.sync_context()
        update = read_context(send.call_args.args[0]["content"][0]["text"])
        assert [message["content"] for message in update["messages"]] == ["new message"]
        assert "memory" not in update
        await voice.sync_context()
        assert send.await_count == 1
    finally:
        await client.ws.close()


async def test_reconnect_keeps_unanswered_inputs_without_executing_them(running):
    gateway, provider, _, port = running
    client = await open_client(port)
    try:
        gateway.store.message("test-session", "user", "unanswered one", id="p1", source="text-user")
        gateway.store.message("test-session", "user", "unanswered two", id="p2", source="text-user")
        await provider.connections[-1].close()
        await client.wait("voice.ready")
        restored = [u for u in voice_updates(provider) if u["snapshot"]][-1]
        assert [m["id"] for m in restored["messages"]] == ["p1", "p2"]
        assert not gateway.tasks.tasks
        assert not any(e["type"] == "speech_text_buffer.commit" for e in provider.received)
    finally:
        await client.ws.close()


async def test_interrupt_records_partial_playback_and_rejects_late_asr(running, monkeypatch):
    gateway, provider, _, port = running
    contexts = capture_prompts(gateway, monkeypatch)
    client = await open_client(port)
    client.auto_playback = False
    try:
        await client.speak(provider, "old budget is 100")
        audio = await client.wait("audio.done")
        await client.send("playback.started", responseId=audio["responseId"])
        voice = gateway.active
        async with asyncio.timeout(2):
            while not voice.responses[audio["responseId"]].get("playback_started"):
                await asyncio.sleep(0.01)
        voice.input_records["late-asr"] = {
            "turn": voice.turn,
            "generation": voice.generation,
            "complete": False,
            "invalid": False,
        }
        await client.send("input.message", text="correction: budget is 500")
        task = (await client.wait("task.completed"))["task"]
        await voice.provider_input(
            {
                "type": "conversation.item.input_audio_transcription.completed",
                "item_id": "late-asr",
                "transcript": "late stale budget 100",
            }
        )
        messages = contexts[0]["messages"]
        assert messages[1]["playbackStatus"] == "interrupted"
        assert task["objective"] == "correction: budget is 500"
        assert "late stale budget 100" not in [m["content"] for m in gateway.store.history(voice.id)]
        await voice.playback_ended(audio["responseId"])
        old = next(m for m in gateway.store.history(voice.id) if m["id"] == "voice:" + audio["responseId"])
        assert old["playbackStatus"] == "interrupted"
    finally:
        await client.ws.close()
