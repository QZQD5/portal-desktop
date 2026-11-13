import asyncio
import base64
import json

import pytest
from conftest import Client, open_client
from conftest import authenticated_connect as connect

from gateway.content import InputAssets, normalize_parts
from gateway.errors import GatewayError


async def action_client(port):
    client = Client(await connect(f"ws://127.0.0.1:{port}/api/realtime?sessionId=actions"))
    await client.send(
        "session.hello",
        protocol={"min": "7.0.0", "max": "7.0.0"},
        client={"type": "web", "instance_id": "actions"},
        connection={"text_only": False, "output_enabled": True},
        capabilities=["input.text", "client.tools", "client.actions.desktop.presence.enter_sleep"],
        tools=[
            {
                "name": "inspect_selection",
                "description": "Inspect selected objects",
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
            }
        ],
    )
    await client.wait("voice.ready")
    return client


async def test_client_tool_and_sleep_acknowledgements(running):
    gateway, speech, _, port = running
    client = await action_client(port)
    try:
        assert any(t["name"] == "inspect_selection" for t in speech.received[0]["session"]["tools"])
        job = asyncio.create_task(gateway.tools.execute("inspect_selection", {}, gateway.active))
        request = await client.wait("client.action.request")
        assert request["name"] == "client.tool.inspect_selection"
        await client.send(
            "client.action.result",
            request_event_id=request["event_id"],
            status="completed",
            output={"selection": ["part-1"]},
        )
        assert await job == {"selection": ["part-1"]}
        await client.send("sleep")
        request = await client.wait("client.action.request")
        assert not gateway.active.sleeping
        await client.send(
            "client.action.result",
            request_event_id=request["event_id"],
            status="failed",
            error={"code": "busy", "message": "Visible work pending"},
        )
        await client.wait("error")
        assert not gateway.active.sleeping
        await client.send("sleep")
        request = await client.wait("client.action.request")
        await client.send("client.action.result", request_event_id=request["event_id"], status="completed")
        assert (await client.wait("voice.sleep"))["state"] == "sleeping"
        await client.send("input.message", text="must wake first")
        assert "休眠" in (await client.wait("error"))["message"]
    finally:
        await client.ws.close()


async def test_actions_timeout_duplicate_and_disconnect(running):
    gateway, _, _, port = running
    client = await action_client(port)
    actions = gateway.active.actions
    try:
        with pytest.raises(GatewayError, match="timed out"):
            await actions.request("client.tool.inspect_selection", {}, timeout=0.1)
        assert not actions.pending
        one = asyncio.create_task(actions.request("client.tool.inspect_selection", {}, key="same"))
        two = asyncio.create_task(actions.request("client.tool.inspect_selection", {}, key="same"))
        await asyncio.sleep(0.03)
        assert len(actions.pending) == 1
        actions.receive(
            {"request_event_id": next(iter(actions.pending)), "status": "completed", "output": 42}
        )
        assert (await one)["output"] == (await two)["output"] == 42
        pending = asyncio.create_task(actions.request("client.tool.inspect_selection", {}))
        await asyncio.sleep(0)
        await client.ws.close()
        with pytest.raises(GatewayError, match="disconnected"):
            await pending
    finally:
        await client.ws.close()


def file_part(mime="text/plain", data=b"hello"):
    return {
        "type": "file",
        "mime": mime,
        "filename": "demo.txt",
        "url": f"data:{mime};base64," + base64.b64encode(data).decode(),
    }


def test_attachment_validation_and_scoping():
    parts = normalize_parts([file_part()])
    assets = InputAssets()
    registered = assets.register("one", parts)
    ref = registered[0]["_meta"]["audio-agent-gateway/inputRef"]
    assert assets.register("one", parts)[0]["_meta"] == registered[0]["_meta"]
    with pytest.raises(ValueError, match="another session"):
        assets.resolve("two", [ref])
    for invalid in (
        {**file_part(), "url": "file:///D:/private.txt"},
        {**file_part(), "mime": "image/png"},
        file_part(data=b"a" * (8 * 1024 * 1024 + 1)),
    ):
        with pytest.raises(ValueError):
            normalize_parts([invalid])


async def test_being_receives_attachment_and_speech_receives_its_result(running):
    gateway, speech, _, port = running
    client = await open_client(port)
    try:
        await client.send("input.message", parts=[{"type": "text", "text": "read attachment"}, file_part()])
        await client.wait("audio.done")
        projection = next(e["text"] for e in speech.received if e["type"] == "speech_text_buffer.commit")
        assert projection.startswith("Being result: ")
        assert "read attachment" in projection
        assert "base64" not in projection
        task = next(iter(gateway.tasks.tasks.values()))
        assert task["parts"][0]["url"] == file_part()["url"]
    finally:
        await client.ws.close()
