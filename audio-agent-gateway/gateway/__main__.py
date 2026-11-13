import argparse
import asyncio
import json
import logging
import os
import socket
import sys
from pathlib import Path

import uvicorn

from .app import create_app
from .config import ROOT, Settings, desktop_settings, validate_setup
from .instance import Lease


async def serve(settings, *, fallback=False, on_ready=None, shutdown=None):
    if not settings.development:
        validate_setup(settings)
    lease = Lease(settings.state).acquire()
    sock, heartbeat = None, None
    try:
        family = socket.AF_INET6 if ":" in settings.host else socket.AF_INET
        sock = socket.socket(family, socket.SOCK_STREAM)
        if os.name == "nt":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        try:
            sock.bind((settings.host, settings.port))
        except OSError:
            if not fallback:
                raise
            sock.bind((settings.host, 0))
        from .audio_activity import preload_model

        preload_model()
        sock.listen(128)
        sock.setblocking(False)
        settings.port = sock.getsockname()[1]
        settings.instance_id = lease.record["instanceId"]
        host = {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(settings.host, settings.host)
        if ":" in host:
            host = f"[{host}]"
        origin = f"http://{host}:{settings.port}"
        if settings.development:
            from .development import create_dev_app
            app = create_dev_app(settings)
        else:
            app = create_app(settings)
        server = uvicorn.Server(
            uvicorn.Config(
                app,
                host=settings.host,
                port=settings.port,
                ws_max_size=20 * 1024 * 1024,
                timeout_graceful_shutdown=10,
                proxy_headers=False,
            )
        )

        async def maintain():
            while not server.started and not server.should_exit:
                await asyncio.sleep(0.05)
            if server.started:
                lease.update(state="ready", origin=origin)
                print(
                    json.dumps(
                        {"type": "gateway.ready", "origin": origin, "gatewayInstanceId": settings.instance_id, "local_history_version": 1}
                    ),
                    flush=True,
                )
                if on_ready:
                    try:
                        on_ready(origin)
                    except Exception:
                        logging.getLogger("gateway").exception("gateway.ready_callback_failed")
            while not server.should_exit:
                await asyncio.sleep(0.25)
                if shutdown and shutdown.is_set():
                    server.should_exit = True
                if not lease.update(state="ready", origin=origin):
                    server.should_exit = True

        heartbeat = asyncio.create_task(maintain())
        await server.serve(sockets=[sock])
    finally:
        if heartbeat:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        if sock:
            sock.close()
        lease.close()


def main():
    parser = argparse.ArgumentParser(description="Portal Desktop voice Gateway")
    parser.add_argument("--dev", action="store_true", help="Run a manually managed local development server")
    parser.add_argument("--port", type=int, default=3102, help="Development port (default: 3102)")
    parser.add_argument("--data-dir", type=Path, default=ROOT / ".runtime/dev", help="Development state directory")
    args = parser.parse_args()
    if args.dev:
        if getattr(sys, "frozen", False):
            parser.error("Development mode is available only from Python source")
        if not 0 <= args.port <= 65535:
            parser.error("Port must be 0-65535")
        settings = Settings(host="127.0.0.1", port=args.port, data_dir=args.data_dir.resolve(), development=True)
        asyncio.run(serve(settings))
        return
    if os.name == "nt":
        # Import native audio dependencies before watch_parent blocks on stdin in
        # a worker thread. Loading NumPy/PyTorch afterwards can hang on Windows.
        from . import audio_activity  # noqa: F401

    # Credentials travel over the private parent pipe, never argv or a bundled .env.
    line = sys.stdin.readline(65537)
    if len(line) > 65536:
        raise ValueError("Desktop configuration too large")
    settings = desktop_settings(json.loads(line))

    async def run():
        shutdown = asyncio.Event()

        async def watch_parent():
            await asyncio.to_thread(sys.stdin.readline)
            shutdown.set()

        watcher = asyncio.create_task(watch_parent())
        try:
            await serve(settings, shutdown=shutdown)
        finally:
            watcher.cancel()
    asyncio.run(run())


if __name__ == "__main__":
    main()
