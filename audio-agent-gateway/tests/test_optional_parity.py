import base64
import json

import pytest
from conftest import Client, open_client
from conftest import authenticated_connect as connect

from gateway.store import now_ms
from gateway.visual import VisualInput


async def test_document_library_removed_without_deleting_existing_data(running, tmp_path):
    gateway, _, http, _ = running
    source = tmp_path / "manual.md"
    source.write_text("existing document", encoding="utf-8")
    documents = [{"id": "old-document", "path": str(source)}]
    gateway.store.put("knowledge_documents", documents)

    assert "knowledge" not in {tool["name"] for tool in gateway.tools.definitions()}
    assert "frontendKnowledge" not in (await http.get("/api/health")).json()
    assert (await http.get("/api/domain")).status_code == 404
    assert (await http.post("/api/domain/import", json={"path": str(source)})).status_code == 404
    assert (await http.delete("/api/domain/old-document")).status_code == 404
    assert gateway.store.get("knowledge_documents") == documents
    assert source.read_text(encoding="utf-8") == "existing document"


async def test_local_memory_removed_without_deleting_existing_data(running):
    gateway, _, http, _ = running
    legacy = gateway.settings.data_dir / "USER.md"
    legacy.write_text("old local memory", encoding="utf-8")

    assert not {"memory", "recall"} & {tool["name"] for tool in gateway.tools.definitions()}
    assert (await http.get("/api/memory")).status_code == 404
    assert (await http.patch("/api/memory", json={"changes": []})).status_code == 404
    assert legacy.read_text(encoding="utf-8") == "old local memory"


async def test_legacy_connect_text_and_missing_event_id(running):
    gateway, _, _, port = running
    client = Client(await connect(f"ws://127.0.0.1:{port}/api/realtime?sessionId=legacy"))
    try:
        await client.ws.send(json.dumps({"type": "connect", "voiceEnabled": True, "clientInstanceId": "old"}))
        await client.wait("voice.ready")
        assert gateway.active.input_enabled
        await client.ws.send(json.dumps({"type": "text.message", "text": "legacy text"}))
        assert (
            "legacy text"
            in (await client.wait("transcript.final", lambda e: e["role"] == "assistant"))["content"]
        )
        assert not any(item["type"] == "session.ready" for item in client.events)
    finally:
        await client.ws.close()


def test_visual_frame_validation_and_provider_drop():
    image = base64.b64encode(b"\xff\xd8\xff\xd9").decode()
    visual = VisualInput()
    assert visual.append({"image": image}) == {"accepted": False, "reason": "provider_unsupported"}
    with pytest.raises(ValueError):
        visual.append({"image": "invalid"})
    forwarded = []
    visual = VisualInput(forwarded.append)
    assert visual.append({"image": image, "occurred_at": 100})["accepted"]
    assert visual.append({"image": image, "occurred_at": 99})["reason"] == "out_of_order"
    assert visual.append({"image": image, "occurred_at": 101})["reason"] == "rate_limited"
    visual.reset()
    assert visual.append({"image": image})["accepted"]




async def test_terminal_retention_preserves_pending_and_active(running):
    gateway, _, _, _ = running
    manager = gateway.tasks
    gateway.settings.max_terminal_tasks = 1
    stamp = now_ms()
    for index, state in enumerate(["completed", "completed", "completed", "running"]):
        manager.tasks[str(index)] = {
            "id": str(index),
            "status": state,
            "createdAt": stamp + index,
            "completedAt": stamp,
            "notificationStatus": "pending" if index == 0 else "none",
        }
    manager.prune()
    assert set(manager.tasks) == {"0", "2", "3"}
    manager.tasks["0"]["completedAt"] = stamp - 8 * 86400000
    manager.prune()
    assert "0" not in manager.tasks
    manager.tasks.clear()


async def test_history_message_id_matches_live_reply(running):
    _, _, http, port = running
    client = await open_client(port)
    try:
        await client.send("input.message", text="match history")
        final = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        history = (await http.get("/api/conversations/test-session/messages")).json()
        messages = history["messages"] if isinstance(history, dict) else history
        assert any(item["id"] == final["messageId"] for item in messages)
        assert final["messageId"] == "voice:" + final["responseId"]
    finally:
        await client.ws.close()
