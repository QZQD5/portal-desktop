"""Crossed cancellation/transport lifetimes, derived from upstream task and ACP code."""

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from conftest import open_client

from gateway.app import Gateway
from conftest import unit_settings as Settings
from gateway.transport import install_windows_cleanup
from gateway.voice import VoiceSession


@pytest.fixture
async def isolated(tmp_path):
    gateway = Gateway(Settings(data_dir=tmp_path, backend_enabled=False, cancel_timeout=0.05))
    manager = gateway.tasks
    yield gateway
    gateway.store.close()


async def active_task(gateway):
    manager = gateway.tasks
    task = await manager.submit("crossed task", "browser")
    task["status"] = "running"
    manager.active = task
    return task


async def test_tool_deduplication_retention_is_bounded(isolated, monkeypatch):
    session = VoiceSession(isolated, None, "retention", {})
    session.responses["response"] = {"calls": {}}
    executed = []

    async def tool(rid, call_id, item):
        executed.append(call_id)

    monkeypatch.setattr(session, "run_tool", tool)
    for index in range(550):
        session.start_tool("response", {"call_id": str(index)})
    await asyncio.gather(*session.tool_tasks)
    session.start_tool("response", {"call_id": "549"})
    assert len(executed) == 550 and len(session.seen_calls) == 500


async def test_reconnect_retires_abandoned_response_state(running):
    gateway, _, _, port = running
    client = await open_client(port)
    session = gateway.active
    try:
        # Interrupted generations may never receive response.done.
        for index in range(160):
            session.responses[f"lost-{index}"] = {
                "done": False,
                "calls": {},
                "followup": False,
                "suppressed": False,
                "generation": session.generation,
            }
        await session.upstream.close()
        await client.wait("voice.ready")
        assert len(session.responses) <= 128
        assert all(info["done"] and info["suppressed"] for info in session.responses.values())
        await client.send("input.message", text="after response cleanup")
        reply = await client.wait("transcript.final", lambda e: e["role"] == "assistant")
        assert reply["content"] == "Being result: after response cleanup"
    finally:
        await client.ws.close()


async def test_reconnect_cleanup_keeps_tool_until_callback_finishes(isolated, monkeypatch):
    session = VoiceSession(isolated, None, "tool-finishing", {})
    entered, release = asyncio.Event(), asyncio.Event()
    session.responses["old"] = {"done": True, "calls": {}, "followup": True}

    async def tool(rid, call_id, item):
        session.responses[rid]["calls"][call_id] = {"status": "completed"}
        entered.set()
        await release.wait()
        assert rid in session.responses

    monkeypatch.setattr(session, "run_tool", tool)
    session.start_tool("old", {"call_id": "pending"})
    job = next(iter(session.tool_tasks))
    await entered.wait()
    for index in range(129):
        session.responses[str(index)] = {"done": True, "calls": {}, "followup": True}
    session.prune_responses()
    try:
        assert "old" in session.responses
    finally:
        release.set()
        await job
    assert not session.tool_tasks


@pytest.mark.skipif(os.name != "nt", reason="Windows Proactor transport")
@pytest.mark.parametrize("reset", [False, True])
@pytest.mark.parametrize("protocol_error", [False, True])
def test_windows_reset_does_not_skip_socket_and_server_cleanup(reset, protocol_error):
    from asyncio.proactor_events import _ProactorBasePipeTransport

    install_windows_cleanup()
    transport = _ProactorBasePipeTransport.__new__(_ProactorBasePipeTransport)
    sock = Mock()
    sock.fileno.return_value = 1
    if reset:
        sock.shutdown.side_effect = ConnectionResetError(10054, "peer reset")
    protocol = Mock()
    if protocol_error:
        protocol.connection_lost.side_effect = RuntimeError("protocol failure")
    server = SimpleNamespace(_detach=Mock())
    transport._sock, transport._server, transport._protocol = sock, server, protocol
    transport._called_connection_lost = False
    if protocol_error:
        with pytest.raises(RuntimeError, match="protocol failure"):
            transport._call_connection_lost(None)
    else:
        transport._call_connection_lost(None)
    transport._call_connection_lost(None)
    sock.close.assert_called_once()
    server._detach.assert_called_once()
    protocol.connection_lost.assert_called_once()
    assert transport._sock is None and transport._server is None and transport._called_connection_lost


@pytest.mark.skipif(os.name != "nt", reason="Windows Proactor transport")
def test_windows_cleanup_preserves_subprocess_pipe_handling():
    from asyncio.proactor_events import _ProactorBasePipeTransport

    install_windows_cleanup()
    transport = _ProactorBasePipeTransport.__new__(_ProactorBasePipeTransport)
    pipe = SimpleNamespace(close=Mock())
    transport._sock, transport._server, transport._protocol = pipe, None, Mock()
    transport._called_connection_lost = False
    transport._call_connection_lost(None)
    pipe.close.assert_called_once()
    assert transport._sock is None and transport._called_connection_lost
