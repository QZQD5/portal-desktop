import asyncio
import copy
import json

import httpx
import pytest

from conftest import mock_text_delegation, open_client, unit_settings
from gateway.app import Gateway
from gateway.being_context import BeingContext
from gateway.conversation_context import context_text
from gateway.store import Store
from test_conversation_context import read_context
from test_local_chat import event, request, Stream


def context(*messages):
    return {"sessionId": "voice", "messages": [
        {"id": str(i), "role": "user", "content": text} for i, text in enumerate(messages)
    ], "tasks": [], "textWork": None, "currentRequest": {"requestId": "request"}}


def test_delta_discards_legacy_memory_and_survives_restart(tmp_path):
    store = Store(tmp_path / "context.db")
    sync = BeingContext(store, "http://being")
    try:
        first = context("already shared")
        first["memory"] = [{"scope": "user", "revision": 1, "content": "old preference"}]
        text, receipt = sync.prepare("scene", sync.capture(first))
        initial = read_context(text)
        assert initial["snapshot"] and initial["memory"] == []
        sync.acknowledge(receipt)
        current = context("already shared", "new voice input")
        current["messages"][0]["playbackStatus"] = "completed"
        current["tasks"] = [{"id": "task", "status": "completed"}]
        text, receipt = sync.prepare("scene", sync.capture(current))
        delta = read_context(text)
        assert not delta["snapshot"]
        assert delta["messages"] == current["messages"]  # Changed metadata is also an update.
        assert "memory" not in delta and delta["tasks"] == current["tasks"]
        assert "textWork" not in delta
        sync.acknowledge(receipt)
        store.close()
        store = Store(tmp_path / "context.db")
        sync = BeingContext(store, "http://being")
        empty, _ = sync.prepare("scene", sync.capture(current))
        assert empty == ""
        assert read_context(sync.prepare("other", sync.capture(current))[0])["messages"] == current["messages"]
        assert read_context(BeingContext(store, "http://other-being").prepare("scene", current)[0])["snapshot"]
    finally:
        store.close()


def test_out_of_order_acknowledgements_do_not_lose_or_roll_back_context(tmp_path):
    store = Store(tmp_path / "context.db")
    sync = BeingContext(store, "http://being")
    try:
        old = sync.capture(context("original"))
        _, old_receipt = sync.prepare("scene", old)
        new = sync.capture(context("corrected", "new input"))
        _, new_receipt = sync.prepare("scene", new)
        sync.acknowledge(new_receipt)
        sync.acknowledge(old_receipt)
        assert sync.prepare("scene", sync.capture(context("corrected", "new input")))[0] == ""
        assert read_context(sync.prepare("scene", old)[0])["messages"] == old["messages"]
        unsent = sync.capture(context("corrected", "new input", "not accepted yet"))
        assert read_context(sync.prepare("scene", unsent)[0])["messages"] == [unsent["messages"][-1]]
        # Preparing a request without an acknowledgement cannot consume its context.
        assert read_context(sync.prepare("scene", unsent)[0])["messages"] == [unsent["messages"][-1]]
    finally:
        store.close()


def test_recipient_filter_keeps_unknown_content_and_handles_explicit_sessions(tmp_path):
    store = Store(tmp_path / "context.db")
    task = {
        "id": "accepted", "objective": "sent goal", "turnId": "old-turn",
        "being": {"endpoint": "http://being", "scene_id": "scene", "session_id": "remote", "submitted": True},
    }
    sync = BeingContext(store, "http://being", tasks={task["id"]: task})
    snapshot = context("sent goal", "keep this unsent constraint", "partial", "complete text", "complete task",
                       "delivered text", "unconfirmed text")
    snapshot["messages"][0].update(source="voice-user", turnId="old-turn")
    snapshot["messages"][1].update(source="voice-user", turnId="old-turn")
    snapshot["messages"][2].update(role="assistant", source="text-being", **{"from": "partial"})
    snapshot["messages"][3].update(role="assistant", source="text-being", beingSessionId="remote", **{"from": "final"})
    snapshot["messages"][4].update(role="assistant", source="agent-result", beingSessionId="remote", **{"from": "being"})
    snapshot["messages"][5].update(source="text-user", deliveryStatus="accepted", beingSessionId="remote")
    snapshot["messages"][6].update(source="text-user", deliveryStatus="unknown", beingSessionId="remote")
    snapshot["tasks"] = [
        {"id": "accepted", "objective": "sent goal", "status": "running"},
        {"id": "local", "objective": "still queued", "status": "queued"},
    ]
    try:
        voice = read_context(context_text(snapshot))
        waiting = read_context(sync.prepare("scene", snapshot, session_id="remote")[0])
        assert waiting["tasks"][0]["objective"] == "sent goal"
        assert waiting["messages"][0]["content"] == "sent goal"
        task["being"]["accepted"] = True
        payload, receipt = sync.prepare("scene", snapshot, session_id="remote")
        same = read_context(payload)
        assert [m["content"] for m in same["messages"]] == ["keep this unsent constraint", "partial", "unconfirmed text"]
        assert same["tasks"] == [{"id": "accepted", "status": "running"}, snapshot["tasks"][1]]
        sync.acknowledge(receipt)
        other = read_context(sync.prepare("scene", snapshot, session_id="other")[0])
        assert other["snapshot"]
        assert other["messages"] == voice["messages"] and other["tasks"] == voice["tasks"] == snapshot["tasks"]
        assert [m["content"] for m in voice["messages"]] == [m["content"] for m in snapshot["messages"]]
        assert all("beingSessionId" not in m for m in voice["messages"])
    finally:
        store.close()


async def test_text_and_delegated_requests_share_delta_and_keep_queued_snapshot(tmp_path):
    gateway = Gateway(unit_settings(data_dir=tmp_path, backend_enabled=False))
    mock_text_delegation(gateway)
    await gateway.tasks.start()
    posted = []
    async def respond(req):
        if req.method == "GET":
            return httpx.Response(204)
        posted.append(json.loads(req.content))
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
                              stream=Stream([event("content_block_delta", {"delta": {"text": "result"}}), event("message_stop", {})]))
    await gateway.tasks.client.http.aclose()
    gateway.tasks.client.http = httpx.AsyncClient(base_url="http://being/", transport=httpx.MockTransport(respond))
    scene = gateway.settings.being_scene["scene_id"]
    async def text(message):
        response = await gateway.chat.submit(request({"message": message, "scene_id": scene}))
        assert response.status_code == 200
        await gateway.chat.job
    try:
        legacy = "old Desktop history"
        gateway.history.import_legacy([{"seq": 1, "role": "user", "content": legacy, "scene_id": scene}])
        gateway.store.message("voice", "user", "first voice", id="first")
        await text("first text")
        assert posted[0]["message"] == "first text"
        assert "<conversation_context>" not in posted[0]["message"]
        gateway.store.message("voice", "user", "second voice", id="second")
        task = await gateway.tasks.submit("queued work", "voice")
        frozen = copy.deepcopy(gateway.store.get("task_context:" + task["id"]))
        assert any(m["content"] == legacy for m in frozen["messages"])
        assert any(m["content"] == "result" for m in frozen["messages"])
        async with asyncio.timeout(2):
            while task["status"] not in {"completed", "failed"}:
                await asyncio.sleep(.01)
        assert task["status"] == "completed"
        assert "<conversation_context>" in posted[1]["message"]
        await text("follow up")
        assert posted[2]["message"] == "follow up"
        assert "<conversation_context>" not in posted[2]["message"]
    finally:
        await gateway.close()


@pytest.mark.parametrize("status", [503, "disconnect", 202])
async def test_http_only_advances_delta_after_acceptance(running, monkeypatch, status):
    gateway, _, http, _ = running
    gateway.store.message("voice", "user", "unshared voice", id="voice")
    posted = []
    send = gateway.tasks.client.http.send
    async def respond(request, **kwargs):
        if request.method == "POST":
            posted.append(json.loads(request.content))
            if len(posted) == 1:
                if status == "disconnect":
                    raise httpx.ReadError("fixture")
                return httpx.Response(status, request=request, json={})
        return await send(request, **kwargs)
    monkeypatch.setattr(gateway.tasks.client.http, "send", respond)
    first = await http.post("/api/heart/chat/stream", json={"message": "first", "scene_id": "fixture-scene"})
    second = await http.post("/api/heart/chat/stream", json={"message": "second", "scene_id": "fixture-scene"})
    assert len(posted) == 2, (first.text, second.text)
    assert posted[0]["message"] == "first" and posted[1]["message"] == "second"
    assert all("<conversation_context>" not in item["message"] for item in posted)


@pytest.mark.parametrize("kind", ["text", "delegated"])
async def test_new_voice_turn_receives_being_results_before_sending_audio(running, monkeypatch, kind):
    gateway, speech, http, port = running
    scene = gateway.settings.being_scene["scene_id"]
    legacy = "old Desktop history"
    gateway.history.import_legacy([{"seq": 1, "role": "user", "content": legacy, "scene_id": scene}])
    client = await open_client(port)
    session = gateway.active
    sync = session.sync_context
    async def delay_sync(*args, **kwargs):
        pass
    monkeypatch.setattr(session, "sync_context", delay_sync)
    try:
        created = next(event["session"] for event in speech.received if event["type"] == "session.create")
        assert legacy in {m["content"] for m in read_context(created["instructions"])["messages"]}
        if kind == "text":
            response = await http.post("/api/heart/chat/stream", json={"message": "work", "scene_id": scene})
            assert response.status_code == 200
        else:
            await gateway.tasks.submit("work", session.id)
            await client.wait("task.completed")
        assert not any(m["content"] == "Being result: work" for m in session.context_sent["messages"])
        monkeypatch.setattr(session, "sync_context", sync)
        monkeypatch.setattr(session.audio_activity, "append", lambda _: True)
        sent_contexts = []
        send = session.upstream_send
        async def capture(event):
            if event["type"] == "input_audio_buffer.append":
                sent_contexts.append(read_context(session.instructions_config["instructions"]))
            await send(event)
        monkeypatch.setattr(session, "upstream_send", capture)
        await session.append_audio("AAAAAA==")
        assert len(sent_contexts) == 1
        assert {legacy, "Being result: work"} <= {m["content"] for m in sent_contexts[0]["messages"]}
    finally:
        await client.ws.close()
