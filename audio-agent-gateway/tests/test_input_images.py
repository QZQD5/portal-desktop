import base64
import json

import httpx
from conftest import open_client

from gateway.store import Store

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jN1sAAAAASUVORK5CYII="
)
IMAGE = {
    "type": "file", "mime": "image/png", "filename": "pixel.png",
    "url": "data:image/png;base64," + base64.b64encode(PNG).decode(),
}


async def test_image_event_history_and_authenticated_bytes(running):
    gateway, _, http, port = running
    await http.get("/api/health")
    client = await open_client(port, text_only=True)
    try:
        await client.send("input.message", parts=[{"type": "text", "text": "describe"}, IMAGE])
        event = await client.wait("transcript.final", lambda e: e["role"] == "user")
        url = event["inputs"][0]["url"]
        response = await http.get("/" + url)
        assert response.status_code == 200
        assert response.content == PNG
        assert response.headers["content-type"] == "image/png"
        assert response.headers["x-content-type-options"] == "nosniff"
        assert "sandbox" in response.headers["content-security-policy"]
        history = (await http.get("/api/conversations/test-session/messages")).json()
        assert history["messages"][0]["inputs"] == event["inputs"]
        store = gateway.store
        for payload in (event, history, store.replay("test-session"), store.journal("test-session")):
            assert "base64," not in json.dumps(payload)
        replay = store.session_replay("test-session")
        assert replay["messages"][0]["inputs"] == event["inputs"]
        assert (await http.get("/" + url.rsplit("/", 1)[0] + "/-1")).status_code == 404
        assert (await http.get("/" + url.replace("test-session", "another-session"))).status_code == 404
        async with httpx.AsyncClient(base_url=http.base_url) as other:
            assert (await other.get("/" + url)).status_code == 401
        store.delete_conversation("test-session")
        assert (await http.get("/" + url)).status_code == 404
    finally:
        await client.ws.close()


def test_legacy_images_recover_from_tasks_and_survive_task_pruning(tmp_path):
    path = tmp_path / "history.sqlite3"
    store = Store(path)
    store.message("chat", "user", "[Image 1] describe", id="user:turn", turnId="turn")
    store.message("chat", "assistant", "answer", turnId="turn")
    store.message("other", "user", "unrelated", turnId="turn")
    store.put("tasks", {"task": {
        "sessionId": "chat", "turnId": "turn", "parts": [IMAGE],
    }})
    store.close()
    store = Store(path)
    try:
        history = store.history("chat")
        assert history[0]["inputs"][0]["url"].endswith("/inputs/0")
        assert "inputs" not in history[1]
        assert "inputs" not in store.history("other")[0]
        store.put("tasks", {})
    finally:
        store.close()
    store = Store(path)
    try:
        assert store.message_input("chat", "user:turn", 0) == IMAGE
        assert "base64," not in json.dumps(store.history("chat"))
    finally:
        store.close()
