from gateway.store import Store, now_ms


def test_conversation_list_uses_saved_messages_and_latest_activity(tmp_path):
    path = tmp_path / "history.sqlite3"
    store = Store(path)
    try:
        assert store.conversations() == []
        store.message("older", "assistant", "welcome", createdAt=5)
        store.message("older", "user", "  First\n question  ", createdAt=10)
        store.message("newer", "user", "Second question", createdAt=20)
        assert [item["sessionId"] for item in store.conversations()] == ["newer", "older"]
        store.message("older", "assistant", "reply", createdAt=30)
        assert store.conversations()[0] == {
            "sessionId": "older", "title": "First question", "updatedAt": 30,
        }
        store.message("attachment", "user", "", createdAt=40)
        assert store.conversations()[0]["title"] == ""
        store.message("long", "user", "x" * 100)
        assert store.conversations()[0]["title"] == "x" * 80
    finally:
        store.close()
    restored = Store(path)
    try:
        assert len(restored.conversations()) == 4
        assert restored.history("older")[-1]["content"] == "reply"
    finally:
        restored.close()


async def test_conversations_endpoint_returns_existing_history(running):
    gateway, _, http, _ = running
    assert (await http.get("/api/conversations")).json() == {"conversations": []}
    gateway.store.message("saved-session", "user", "Saved conversation")
    response = await http.get("/api/conversations")
    assert response.status_code == 200
    assert response.json()["conversations"][0]["sessionId"] == "saved-session"
    assert response.json()["conversations"][0]["title"] == "Saved conversation"


async def test_delete_conversation_clears_history_and_tasks_without_touching_others(running):
    gateway, _, http, _ = running
    gateway.store.message("delete-me", "user", "Delete this")
    gateway.store.message("keep-me", "user", "Keep this")
    gateway.store.event("delete-me", {"type": "test"})
    task = await gateway.tasks.submit(
        "work", "delete-me", schedule={"type": "reminder", "executeAt": now_ms() + 100000}
    )
    response = await http.delete("/api/conversations/delete-me")
    assert response.status_code == 409
    assert gateway.store.history("delete-me")
    task.update(status="completed", notificationStatus="pending", completedAt=now_ms())
    assert (await http.delete("/api/conversations/delete-me")).status_code == 200
    assert gateway.store.history("delete-me") == []
    assert gateway.store.replay("delete-me")["events"] == []
    assert task["id"] not in gateway.tasks.tasks
    assert gateway.store.get("task_context:" + task["id"]) is None
    gateway.store.message("delete-me", "assistant", "late reply")
    assert gateway.store.history("delete-me") == []
    assert [c["sessionId"] for c in gateway.store.conversations()] == ["keep-me"]
    assert (await http.delete("/api/conversations/delete-me")).status_code == 200
