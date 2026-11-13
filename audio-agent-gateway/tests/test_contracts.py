
from conftest import open_client



async def test_multiple_input_owners_do_not_release_each_other(running):
    _, _, http, _ = running
    assert (await http.post("/api/input/suspend", json={})).status_code == 400
    await http.post("/api/input/suspend", json={"owner": "one", "ttlMs": 1000})
    await http.post("/api/input/suspend", json={"owner": "two", "ttlMs": 1000})
    status = (await http.post("/api/input/resume", json={"owner": "one"})).json()
    assert status["suspended"] and status["owner"] == "two"
    status = (await http.post("/api/input/resume", json={"owner": "two"})).json()
    assert not status["suspended"]




async def test_http_journal_projection_and_message_ids(running):
    _, _, http, port = running
    client = await open_client(port)
    try:
        await client.send("input.message", text="persist me")
        await client.wait("audio.done")
        await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        journal = (await http.get("/api/sessions/test-session/events")).json()["events"]
        assert journal[0]["schema"] == "qwaudio.session/v1"
        assert any(e["type"] == "user/message" for e in journal[1:])
        replay = (await http.get("/api/sessions/test-session/replay")).json()["replay"]
        assistant = next(x for x in replay["messages"] if x["role"] == "assistant")
        final = next(e for e in client.events if e["type"] == "transcript.final" and e["role"] == "assistant")
        assert assistant["id"] == final["messageId"]
    finally:
        await client.ws.close()


async def test_tool_citations_reach_final_transcript_and_history(running, monkeypatch):
    gateway, speech, _, port = running

    async def retrieval_result(*args, **kwargs):
        return {"status": "ok", "citations": [{"id": "1", "title": "Example", "url": "https://example.com/"}]}

    monkeypatch.setattr(gateway.tools, "execute", retrieval_result)
    client = await open_client(port)
    try:
        await client.speak(speech, "delegate")
        event = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert event["citations"][0]["url"] == "https://example.com/"
        assert gateway.store.history("test-session")[-1]["citations"] == event["citations"]
    finally:
        await client.ws.close()
