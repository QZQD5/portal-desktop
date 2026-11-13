"""Local manual-start mode; Desktop supplies only the Being binding."""
import asyncio
import hashlib
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .access import authorized
from .app import create_app
from .config import desktop_settings, validate_setup
from .logging import configure


def create_dev_app(settings):
    configure(settings.data_dir / "logs")
    current = lifetime = configured = None
    lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            if lifetime:
                await lifetime.__aexit__(None, None, None)

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def authenticate(request, call_next):
        if not authorized(request, settings):
            return JSONResponse({"error": "Unauthorized"}, 401)
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/api/dev/status")
    async def status():
        return {"kind": "audio-agent-gateway-dev", "protocol_version": "7.0.0", "local_history_version": 1, "configured": current is not None}

    @app.post("/api/dev/configure")
    async def bind(request: Request):
        nonlocal current, lifetime, configured
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 65536:
                return JSONResponse({"error": "Configuration too large"}, 413)
        try:
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("Expected an object")
            next_settings = desktop_settings({**value, "data_dir": str(settings.data_dir)}, development=True)
        except (ValueError, TypeError):
            return JSONResponse({"error": "Invalid Desktop configuration"}, 400)
        next_settings.doubao_api_key = settings.doubao_api_key
        try:
            validate_setup(next_settings)
        except (ValueError, TypeError):
            return JSONResponse({"error": "Invalid Desktop configuration"}, 400)
        scope = hashlib.sha256(next_settings.being_endpoint.encode()).hexdigest()
        next_settings.data_dir = settings.data_dir / scope
        scenes = value.get("being_scenes", [next_settings.being_scene])
        if not isinstance(scenes, list) or len(scenes) > 500 or any(
                not isinstance(scene, dict) or not isinstance(scene.get("scene_id"), str) for scene in scenes):
            return JSONResponse({"error": "Invalid scenes"}, 400)
        next_settings.legacy_history = [{"path": str(settings.data_dir / hashlib.sha256(
            (next_settings.being_endpoint + "\n" + scene["scene_id"]).encode()).hexdigest() / "gateway.sqlite3"),
            "scene": scene} for scene in scenes]
        async with lock:
            if configured == next_settings and current:
                return {"ok": True, "protocol_version": "7.0.0"}
            if current and current.state.gateway.active and not current.state.gateway.active.closed:
                return JSONResponse({"error": "End the current voice session before changing the binding"}, 409)
            current, configured = None, None
            if lifetime:
                previous, lifetime = lifetime, None
                await previous.__aexit__(None, None, None)
            candidate = create_app(next_settings)
            context = candidate.router.lifespan_context(candidate)
            await context.__aenter__()
            current, lifetime, configured = candidate, context, next_settings
        return {"ok": True, "protocol_version": "7.0.0"}

    async def forward(scope, receive, send):
        target = current
        if target:
            await target(scope, receive, send)
        elif scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
        else:
            await JSONResponse({"error": "Waiting for Desktop configuration"}, 503)(scope, receive, send)

    app.mount("/", forward)
    return app
