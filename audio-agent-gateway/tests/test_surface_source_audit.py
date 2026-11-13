import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

from conftest import open_client

from conftest import unit_settings as Settings
from gateway.external_tools import mcp_result, openapi_result_data
from gateway.search import normalize, project_citations, search, search_response
from gateway.tools import page_response
from gateway.voice import Reply

ORACLE = json.loads(
    (Path(__file__).parent / "fixtures/surface-upstream-oracle.json").read_text(encoding="utf-8")
)


def test_original_mcp_result_bounds_and_empty_fields():
    for case in ORACLE["mcp"]:
        response = case["response"]
        value = SimpleNamespace(
            content=[SimpleNamespace(**block) for block in response["content"]],
            isError=response.get("isError", False),
            structuredContent=response.get("structuredContent"),
        )
        assert mcp_result(value, case["maxBytes"]) == case["result"]


async def test_original_notes_error_receipts(running):
    gateway, _, _, _ = running
    for case in ORACLE["notes"]:
        assert gateway.tools.notes(case["args"]) == case["result"]


def test_openapi_json_detection_respects_content_type():
    assert openapi_result_data("123", "text/plain") == {"text": "123"}
    assert openapi_result_data("123", "application/json") == {"structured_content": 123}
    assert openapi_result_data(" {}", "text/plain") == {"structured_content": {}}
    assert openapi_result_data("invalid", "application/json") == {"text": "invalid"}


async def test_search_keeps_full_query_and_skips_invalid_results(monkeypatch):
    assert (
        normalize([{"url": "http://[invalid"}, {"url": "https://valid.example"}], 5)[0]["url"]
        == "https://valid.example"
    )

    async def provider(settings, query, limit):
        assert query == "字" * 550
        return []

    monkeypatch.setattr("gateway.search.mcp_search", provider)
    result = await search(Settings(web_search_provider="mcp"), "字" * 550, 5, None)
    assert result["query"] == "字" * 500


def test_original_search_and_page_projection():
    for case in ORACLE["retrieval"]:
        assert search_response(case["items"], case["query"]) == case["result"]
    for case in ORACLE["pages"]:
        assert page_response(case["url"], case["body"], case["media_type"]) == case["result"]
    existing = []
    for case in ORACLE["turns"]:
        assert project_citations(existing, case["result"]) == case["expected"]


async def test_http_liveness_origins_limits_and_suspend_errors(running):
    gateway, _, http, _ = running
    assert (await http.get("/livez")).json() == {"ok": True, "status": "live"}
    assert (await http.get("/readyz")).json() == {"ok": True, "status": "ready"}
    assert (await http.get("/api/health", headers={"origin": "http://localhost"})).status_code == 401
    assert (await http.get("/")).status_code == 404
    assert (
        await http.post("/api/input/suspend", json={"padding": "x" * (1024 * 1024)})
    ).status_code == 413
    missing = await http.post("/api/input/suspend", json={})
    assert missing.status_code == 400 and missing.json()["code"] == "QWAUDIO_INPUT_OWNER_REQUIRED"
    for value in ["bad", "Infinity", -3, 0, None]:
        response = await http.post("/api/input/suspend", json={"owner": "audit", "ttlMs": value})
        assert response.status_code == 200 and response.json()["holders"][0]["ttlMs"] == 15000
    await http.post("/api/input/resume", json={"owner": "audit"})
    health = (await http.get("/api/health")).json()
    assert health["realtimeModelProfile"]["id"] == "1.2.6.1"
    assert health["realtimeModelCatalog"] == [health["realtimeModelProfile"]]
    assert "frontendMemory" not in health
    assert health["frontendRetrieval"]["searchProvider"]["key"] == gateway.settings.web_search_provider
    assert health["announcementBatchMs"] == gateway.settings.announcement_batch_window * 1000
    assert health["backend"]["transport"] == "http-sse"
    assert health["voiceClients"]["activeClients"] == 0
    assert health["voiceClients"]["byType"] == {"desktop": 0, "cli": 0, "web": 0}
    assert health["voiceClients"]["realtime"]["byProvider"] == {}


async def test_seeduplex_voice_selection_and_independent_response_deadlines(running):
    gateway, speech, http, port = running
    client = await open_client(port)
    try:
        status = (await http.get("/api/health")).json()["voiceClients"]
        assert status["activeClients"] == 1
        assert status["realtime"]["byProvider"]["doubao-seeduplex"]["connected"] == 1
        await client.send("session.output_voice.update", voice="test-voice")
        updated = await client.wait("session.output_voice.updated")
        assert updated["voice"] == "test-voice"
        await asyncio.sleep(0.05)
        assert any(
            event.get("session", {}).get("audio", {}).get("output", {}).get("voice") == "test-voice"
            for event in speech.received
        )
        session = gateway.active
        reply = Reply(session.generation, requested_at=time.monotonic() - 5)
        session.last_activity = time.monotonic()
        assert session.reply_stalled(reply)
        session.responses["test-deadline"] = {"reply": reply, "audio": True, "last_activity": time.monotonic() - 10}
        assert not session.reply_stalled(reply)
        session.responses["test-deadline"]["last_activity"] = time.monotonic() - 25
        assert session.reply_stalled(reply)
        del session.responses["test-deadline"]
        await client.send("input.message", text="still usable after unsupported voice")
        await client.wait("transcript.final", lambda e: e["role"] == "assistant")
    finally:
        await client.ws.close()
