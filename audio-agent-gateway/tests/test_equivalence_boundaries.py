import asyncio
from types import SimpleNamespace

import pytest
from conftest import Client, open_client
from conftest import authenticated_connect as connect

from gateway.client_events import ClientEvents
from conftest import unit_settings as Settings
from gateway.errors import GatewayError
from gateway.logging import redact
from gateway.store import Store, now_ms


async def test_named_events_bound_routes_deduplicate_and_keep_latest():
    delivered, handled = [], []

    async def deliver(value):
        delivered.append(value)

    session = SimpleNamespace(
        id="s", settings=Settings(), hello={"client": {"instance_id": "c"}}, deliver_client_event=deliver
    )
    router = ClientEvents(
        [
            {
                "name": "editor.selection",
                "schema": {"type": "object", "required": ["id"]},
                "route": "context",
                "retention": "latest",
                "coalesceKey": None,
                "handle": handled.append,
                "project": lambda event: {"mode": "interrupt", "text": "selection"},
                "rateLimit": {"max": 2, "windowMs": 10000},
            }
        ]
    )
    event = {"event_id": "one", "name": "editor.selection", "data": {"id": 1}, "delivery_hint": "interrupt"}
    assert not (await router.publish(session, event))["duplicate"]
    assert delivered[0]["delivery_hint"] == handled[0]["route"] == "context"
    assert (await router.publish(session, event))["duplicate"]
    await router.publish(session, {**event, "event_id": "two", "data": {"id": 2}})
    assert len(router.latest) == 1 and next(iter(router.latest.values()))["data"] == {"id": 2}
    with pytest.raises(GatewayError, match="rate limit"):
        await router.publish(session, {**event, "event_id": "three"})
    with pytest.raises(GatewayError, match="Invalid Client Event data"):
        await router.publish(session, {**event, "event_id": "bad", "data": {}})
    assert len(handled) == len(delivered) == 2


async def test_mute_releases_provider_and_unmute_reconnects(running):
    gateway, speech, _, port = running
    client = await open_client(port)
    try:
        await client.send("mute")
        await client.wait("voice.connection", lambda e: e["state"] == "disconnected")
        await asyncio.sleep(0.6)
        assert gateway.active.upstream is None and not gateway.active.ready.is_set()
        assert len(speech.connections) == 1
        await client.send("unmute")
        await client.wait("voice.ready")
        assert len(speech.connections) == 2 and gateway.active.output_enabled
        await client.send("input.message", text="after reconnect")
        assert (
            "after reconnect"
            in (await client.wait("transcript.final", lambda e: e["role"] == "assistant"))["content"]
        )
    finally:
        await client.ws.close()


async def test_disabled_initial_audio_does_not_claim_speech_slot(running):
    _, speech, _, port = running
    client = Client(await connect(f"ws://127.0.0.1:{port}/api/realtime"))
    try:
        await client.send(
            "session.hello",
            protocol={"min": "7.0.0", "max": "7.0.0"},
            client={"type": "web", "instance_id": "silent"},
            capabilities=["input.text"],
            connection={"input_enabled": False, "output_enabled": False},
        )
        await client.wait("session.ready")
        await asyncio.sleep(0.1)
        assert not speech.connections
        await client.send("input.message", text="connect on demand")
        await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert not speech.connections
    finally:
        await client.ws.close()


async def test_reconfigure_connect_and_legacy_unknown_event(running):
    gateway, _, http, port = running
    client = await open_client(port)
    try:
        await client.send("connect", voiceEnabled=True, outputVoice="test-voice")
        await client.wait("voice.ready")
        assert gateway.active.output_voice == "test-voice"
        assert not gateway.active.hello["legacy"]
        assert sum(e["type"] == "session.ready" for e in client.events) == 1
        capabilities = (await http.get("/api/health")).json()["capabilities"]
        assert "realtime.gateway-client-protocol-v6-client-actions" in capabilities
    finally:
        await client.ws.close()
    legacy = Client(await connect(f"ws://127.0.0.1:{port}/api/realtime"))
    try:
        await legacy.ws.send('{"type":"unknown.before.connect"}')
        await legacy.ws.send('{"type":"connect","voiceEnabled":true}')
        await legacy.wait("voice.ready")
        await legacy.ws.send('{"type":"unknown.after.connect"}')
        await legacy.ws.send('{"type":"audio.append","audio":null}')
        await legacy.ws.send('{"type":"text.message","text":"still usable"}')
        await legacy.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert not any(e["type"] == "error" for e in legacy.events)
    finally:
        await legacy.ws.close()


def test_logs_redact_url_secrets():
    assert "private-key" not in redact("https://example.test/?token=private-key&x=ok")
    assert "password" not in redact("Basic password")


def test_journal_compaction_keeps_recovery_and_reclaims_old_sessions(tmp_path):
    store = Store(tmp_path / "test.sqlite3")
    try:
        recover = {"id": "unfinished", "status": "completed", "notificationStatus": "pending"}
        store.event("protected", {"type": "task.completed", "task": recover, "occurred_at": 1})
        for index in range(12):
            store.event(
                "protected",
                {"type": "transcript.final", "role": "user", "content": str(index), "messageId": str(index)},
            )
        with store.db:
            store.compact("protected", max_events=5)
        events = store.journal("protected")
        assert events[0]["retention"]["discardedEvents"] >= 8
        assert any(item.get("payload", {}).get("task", {}).get("id") == "unfinished" for item in events)
        assert len(events) <= 6
        store.event("old", {"type": "transcript.final", "role": "user", "content": "old", "occurred_at": 1})
        store.event(
            "new", {"type": "transcript.final", "role": "user", "content": "new", "occurred_at": now_ms()}
        )
        store.maintain(max_sessions=1)
        assert len(store.journal("old")) == 1
        assert len(store.journal("protected")) > 1
        assert len(store.journal("new")) == 1
        # Capacity failure must preserve all required recovery facts.
        before = store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        with pytest.raises(GatewayError, match="budget"), store.db:
            store.compact("protected", max_events=1)
        assert store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == before
    finally:
        store.close()
