"""Owner-scoped event admission; handlers are configured by the server, never by clients."""

import inspect
import json
import re
import time

import jsonschema

from .errors import GatewayError

PRIORITY = {"handle": 0, "context": 1, "respond": 2, "interrupt": 3}
RESERVED = {"client", "conversation", "gateway", "permission", "response", "session", "task"}


class ClientEvents:
    def __init__(self, definitions=()):
        self.definitions, self.seen, self.rates, self.latest = {}, {}, {}, {}
        for definition in definitions:
            name = definition.get("name", "")
            if (
                not re.fullmatch(r"[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)+", name)
                or name.split(".")[0] in RESERVED
                or name in self.definitions
            ):
                raise ValueError("Invalid, reserved or duplicate Client Event name")
            jsonschema.Draft202012Validator.check_schema(definition["schema"])
            self.definitions[name] = {
                **definition,
                "route": definition.get("route") if definition.get("route") in PRIORITY else "handle",
            }

    async def publish(self, session, message):
        text_event = "text" in message
        definition = (
            {
                "name": "client.context",
                "route": "interrupt",
                "maxBytes": 32768,
                "rateLimit": {"max": 20, "windowMs": 10000},
            }
            if text_event
            else self.definitions.get(message.get("name"))
        )
        if definition is None:
            raise GatewayError("client_event_unsupported", "Unsupported Client Event")
        source = (session.id, session.hello.get("client", {}).get("instance_id", ""))
        now = time.monotonic()
        self.seen = {key: at for key, at in self.seen.items() if now - at < 300}
        self.rates = {key: value for key, value in self.rates.items() if now - value[0] < 60}
        while len(self.rates) >= 1024:
            del self.rates[next(iter(self.rates))]
        duplicate = (*source, message["event_id"])
        if duplicate in self.seen:
            return {
                "accepted": True,
                "duplicate": True,
                **({"name": message["name"]} if "name" in message else {}),
            }
        data = message["text"] if text_event else message.get("data", {})
        if len(json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()) > min(
            65536, max(128, definition.get("maxBytes", 4096))
        ):
            raise GatewayError("payload_too_large", "Client Event payload exceeds byte limit")
        if text_event:
            if not isinstance(data, str) or not 1 <= len(data.strip()) <= 16000:
                raise GatewayError("client_event_invalid", "Invalid Client Event text")
            data = data.strip()
        else:
            try:
                jsonschema.validate(data, definition["schema"])
            except jsonschema.ValidationError:
                raise GatewayError("client_event_invalid", "Invalid Client Event data") from None
        rate = definition.get("rateLimit", {})
        rate_key = (*source, definition["name"])
        started, count = self.rates.get(rate_key, (now, 0))
        if now - started >= min(60000, max(100, rate.get("windowMs", 1000))) / 1000:
            started, count = now, 0
        self.rates[rate_key] = (started, count + 1)
        if count >= min(1000, max(1, rate.get("max", 20))):
            raise GatewayError("rate_limited", "Client Event rate limit exceeded")
        maximum = definition.get("route", "handle")
        route = message.get("delivery_hint", "context" if text_event else maximum)
        if PRIORITY[route] > PRIORITY[maximum]:
            route = maximum
        event = {
            "id": message["event_id"],
            "name": message.get("name"),
            "data": data,
            "route": route,
            "occurredAt": message.get("occurred_at"),
            "source": {
                "ownerId": session.settings.owner_id,
                "sessionId": session.id,
                "clientType": session.hello.get("client", {}).get("type", "web"),
                "clientInstanceId": source[1],
            },
        }
        if handler := definition.get("handle"):
            outcome = handler(event)
            if inspect.isawaitable(outcome):
                await outcome
        delivery = None
        if text_event:
            delivery = {"text": data, "delivery_hint": route}
        elif projector := definition.get("project"):
            delivery = projector(event)
            if inspect.isawaitable(delivery):
                delivery = await delivery
        self.seen[duplicate] = now
        while len(self.seen) > 1024:
            del self.seen[next(iter(self.seen))]
        if definition.get("retention") == "latest":
            coalesce = (
                definition["coalesceKey"](data, event["source"])
                if callable(definition.get("coalesceKey"))
                else ""
            )
            self.latest[(*source, definition["name"], coalesce)] = event
            while len(self.latest) > 1024:
                del self.latest[next(iter(self.latest))]
        if delivery and route != "handle":
            mode = delivery.get("mode", delivery.get("delivery_hint", route))
            mode = mode if mode in PRIORITY and PRIORITY[mode] <= PRIORITY[route] else route
            if mode != "handle":
                await session.deliver_client_event({**delivery, "delivery_hint": mode})
        return {
            "accepted": True,
            "duplicate": False,
            **({"name": message["name"]} if "name" in message else {}),
        }
