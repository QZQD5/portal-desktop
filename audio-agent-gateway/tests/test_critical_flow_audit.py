"""Source-first checks for ingress, task observers, network and partial startup."""

import asyncio
import gc
import socket
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from gateway.app import Gateway, create_app
from gateway.client_actions import ClientActions
from conftest import unit_settings as Settings
from gateway.tools import public_addresses


@pytest.mark.parametrize("media", [None, "text/plain", "application/octet-stream"])
async def test_non_json_body_cannot_execute_input_command(tmp_path, media):
    app = create_app(Settings(data_dir=tmp_path, backend_enabled=False))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://localhost", headers={"Authorization": "Bearer unit-desktop-key"}) as http,
    ):
        response = await http.post(
            "/api/input/suspend",
            content='{"owner":"wrong-media"}',
            headers={"Content-Type": media} if media else {},
        )
        assert response.status_code == 400
        assert not app.state.gateway.suspended


@pytest.mark.parametrize(
    "media", ["application/json", "Application/JSON", "application/json ; charset=utf-8"]
)
async def test_json_body_limit_applies_to_all_valid_media_spellings(tmp_path, media):
    app = create_app(Settings(data_dir=tmp_path, backend_enabled=False))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://localhost", headers={"Authorization": "Bearer unit-desktop-key"}) as http,
    ):
        response = await http.post(
            "/api/input/suspend",
            content='{"owner":"' + "x" * (1024 * 1024) + '"}',
            headers={"Content-Type": media},
        )
        assert response.status_code == 413
        assert not app.state.gateway.suspended


@pytest.mark.parametrize(
    "address,family",
    [
        ("224.0.0.1", socket.AF_INET),
        ("ff02::1", socket.AF_INET6),
        ("::ffff:224.0.0.1", socket.AF_INET6),
    ],
)
def test_retrieval_blocks_multicast_before_connecting(monkeypatch, address, family):
    monkeypatch.setattr(
        socket, "getaddrinfo", lambda *a, **k: [(family, socket.SOCK_STREAM, 6, "", (address, 80))]
    )
    with pytest.raises(ValueError, match="public internet"):
        public_addresses("untrusted.example", 80)


async def test_notification_failure_cannot_strand_task_queue_or_cancellation(tmp_path):
    gateway = Gateway(Settings(data_dir=tmp_path, backend_enabled=False))
    manager = gateway.tasks
    manager.emit = AsyncMock(side_effect=RuntimeError("observer failed"))
    manager.client.run = AsyncMock(side_effect=ConnectionError("Being unavailable"))
    try:
        first = await manager.submit("first", "s")
        second = await manager.submit("second", "s")
        await manager.cancel(second["id"])
        await manager.start()
        async with asyncio.timeout(2):
            while first["status"] not in {"failed", "completed"} or manager.active:
                await asyncio.sleep(0.01)
        assert first["status"] == "failed" and first["completedAt"]
        assert second["status"] == "cancelled"
        assert manager.store.get("tasks")[first["id"]]["status"] == "failed"
    finally:
        await gateway.close()


async def test_failed_app_start_reaps_started_workers(tmp_path, monkeypatch):
    original = Gateway.start
    captured = []

    async def fail(owners):
        captured.append(owners)
        await original(owners)
        raise RuntimeError("startup interrupted")

    monkeypatch.setattr(Gateway, "start", fail)
    app = create_app(Settings(data_dir=tmp_path, backend_enabled=False))
    try:
        with pytest.raises(RuntimeError, match="startup interrupted"):
            async with app.router.lifespan_context(app):
                pytest.fail("failed startup yielded")
        owners = captured[0]
        assert owners.tasks.worker.done()
        with pytest.raises(sqlite3.ProgrammingError):
            owners.store.get("tasks")
    finally:
        if captured and not captured[0].tasks.worker.done():
            await captured[0].close()


async def test_cleanup_failure_does_not_skip_remaining_resources(tmp_path, monkeypatch):
    gateway = Gateway(Settings(data_dir=tmp_path, backend_enabled=False))
    original = gateway.tasks.close

    async def broken():
        await original()
        raise RuntimeError("task cleanup failed")

    monkeypatch.setattr(gateway.tasks, "close", broken)
    external = AsyncMock()
    monkeypatch.setattr(gateway.tools.external, "close", external)
    with pytest.raises(RuntimeError, match="task cleanup failed"):
        await gateway.close()
    external.assert_awaited_once()
    with pytest.raises(sqlite3.ProgrammingError):
        gateway.store.get("tasks")


@pytest.mark.parametrize("failure", ["disconnect", "client_error"])
async def test_client_action_failure_and_cancellation_leave_no_unhandled_future(failure):
    sent = asyncio.Event()

    async def emit(event):
        sent.set()

    actions = ClientActions(
        SimpleNamespace(
            hello={"capabilities": ["client.tools"]},
            client_tools={"probe": {}},
            emit=emit,
        )
    )
    loop, errors = asyncio.get_running_loop(), []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, context: errors.append(context))
    try:
        job = asyncio.create_task(actions.request("client.tool.probe"))
        await sent.wait()
        if failure == "disconnect":
            actions.close()
        else:
            actions.receive(
                {
                    "request_event_id": next(iter(actions.pending)),
                    "status": "failed",
                    "error": {"code": "failed", "message": "failed"},
                }
            )
        job.cancel()
        await asyncio.gather(job, return_exceptions=True)
        gc.collect()
        await asyncio.sleep(0)
        assert not errors and not actions.pending
    finally:
        loop.set_exception_handler(previous)


async def test_websocket_cancel_ack_does_not_wait_for_agent_transport(tmp_path):
    gateway = Gateway(Settings(data_dir=tmp_path, backend_enabled=False, cancel_timeout=5))
    manager = gateway.tasks
    task = await manager.submit("cancel", "s")
    task["status"] = "running"
    manager.active = task
    release = asyncio.Event()

    async def notify(*args):
        await release.wait()

    manager.client = SimpleNamespace(ready=True, cancel=notify, close=AsyncMock())
    session = SimpleNamespace(hello={"capabilities": ["tasks.commands"]})
    try:
        result = await asyncio.wait_for(
            gateway.command(
                session,
                {
                    "type": "task.cancel",
                    "event_id": "cancel-now",
                    "task_id": task["id"],
                },
            ),
            0.2,
        )
        assert result["task"]["status"] == "cancelling"
        assert not manager.cancellations[task["id"]].done()
    finally:
        release.set()
        await asyncio.gather(*manager.cancellations.values(), return_exceptions=True)
        manager.active = None
        await gateway.close()
