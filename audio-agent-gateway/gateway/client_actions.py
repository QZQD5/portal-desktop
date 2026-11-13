"""Connection-local GCP actions; only the client can acknowledge an operation."""

import asyncio

from .errors import GatewayError
from .store import uid

ENTER_SLEEP = "desktop.presence.enter_sleep"


class ClientActions:
    def __init__(self, session):
        self.session = session
        self.pending = {}
        self.keys = {}

    def supports(self, name):
        caps = self.session.hello.get("capabilities", [])
        if name.startswith("client.tool."):
            return "client.tools" in caps and name[12:] in self.session.client_tools
        return name == ENTER_SLEEP and (
            "client.actions." + name in caps or "sleeping" in self.session.client_states
        )

    async def request(self, name, arguments=None, *, key="", timeout=5):
        if not self.supports(name):
            raise GatewayError("client_action_unsupported", f"Client does not support {name}")
        if key and key in self.keys:
            return await asyncio.shield(self.keys[key])
        request_id = uid("action")
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        if key:
            self.keys[key] = future
        try:
            await self.session.emit(
                {
                    "type": "client.action.request",
                    "event_id": request_id,
                    "name": name,
                    "arguments": arguments or {},
                }
            )
            try:
                async with asyncio.timeout(max(0.1, timeout)):
                    return await asyncio.shield(future)
            except TimeoutError:
                error = GatewayError("client_action_timeout", f"Client Action timed out: {name}")
                if not future.done():
                    future.set_exception(error)
                    future.exception()
                raise error from None
        finally:
            self.pending.pop(request_id, None)
            if key:
                self.keys.pop(key, None)
            if not future.done():
                future.cancel()

    def receive(self, result):
        future = self.pending.get(result["request_event_id"])
        if future is None or future.done():
            return False
        if result["status"] == "completed":
            future.set_result(result)
        else:
            error = result["error"]
            future.set_exception(GatewayError(error["code"], error["message"]))
        return True

    def close(self):
        for future in self.pending.values():
            if not future.done():
                future.set_exception(GatewayError("client_action_disconnected", "Client disconnected"))
