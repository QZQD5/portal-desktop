"""Frozen upstream schemas plus checks that Zod refinements cannot export to JSON."""

import json

from jsonschema import Draft202012Validator

from .config import ROOT, desktop_display_name
from .errors import GatewayError
from .store import uid

SCHEMAS = json.loads((ROOT / "gateway/resources/client-protocol.json").read_text(encoding="utf-8"))
VALIDATORS = {key: Draft202012Validator(value) for key, value in SCHEMAS.items()}
COMMANDS = dict(
    zip(
        [
            "session.pong",
            "task.create",
            "task.get",
            "task.list",
            "task.cancel",
            "permission.respond",
            "task.input.respond",
            "conversation.history",
            "session.replay",
            "session.output_voice.update",
            "client.event.publish",
            "client.action.result",
        ],
        [
            "GatewaySessionPongSchema",
            "GatewayTaskCreateSchema",
            "GatewayTaskGetSchema",
            "GatewayTaskListSchema",
            "GatewayTaskCancelSchema",
            "GatewayPermissionRespondSchema",
            "GatewayInputRespondSchema",
            "GatewayConversationHistorySchema",
            "GatewaySessionReplaySchema",
            "GatewaySessionOutputVoiceUpdateSchema",
            "GatewayClientEventPublishSchema",
            "GatewayClientActionResultSchema",
        ],
        strict=True,
    )
)


def check(name, value):
    error = next(VALIDATORS[name].iter_errors(value), None)
    if error:
        location = ".".join(map(str, error.absolute_path)) or "event"
        # Do not echo payloads (which can contain user text) into error logs.
        raise GatewayError("invalid_event", f"Invalid {location}: {error.validator}")


def hello(value):
    check("GatewaySessionHelloSchema", value)
    if "being_name" in value:
        desktop_display_name(value["being_name"])
    capabilities = value["capabilities"]
    if len(set(capabilities)) != len(capabilities):
        raise ValueError("Duplicate capabilities")
    if value.get("tools") and "client.tools" not in capabilities:
        raise ValueError("tools require client.tools capability")
    tools = value.get("tools", [])
    if len({tool["name"] for tool in tools}) != len(tools):
        raise ValueError("Duplicate client tool")
    if any(len(json.dumps(tool["inputSchema"], ensure_ascii=False).encode()) > 16384 for tool in tools):
        raise ValueError("Tool schema too large")
    connection = value.get("connection", {})
    if connection.get("takeover") and "session.takeover" not in capabilities:
        raise ValueError("takeover requires session.takeover capability")
    if connection.get("provider", "doubao-seeduplex") not in {
        "doubao-seeduplex",
        "doubao",
        "seeduplex",
        "volcengine",
    }:
        raise ValueError("This deployment only provides Doubao Seeduplex")


def event(value):
    check("GatewayClientEnvelopeSchema", value)
    kind = value["type"]
    if kind in COMMANDS:
        check(COMMANDS[kind], value)
    if kind == "client.action.result" and value["status"] != "completed" and not value.get("error"):
        raise ValueError("Failed and unsupported Client Actions require an error")
    if kind == "client.presence.update" and value.get("state") not in {"active", "sleeping"}:
        raise ValueError("Invalid presence state")
    business_event(value)
    if kind == "client.event.publish":
        if not value.get("text") and not value.get("name"):
            raise ValueError("Client Event requires text or a registered extension name")
        if "text" in value and ("data" in value or value.get("delivery_hint") == "handle"):
            raise ValueError("Text events cannot invoke handlers or carry handler data")


def business_event(value):
    kind = value.get("type")

    def string(key, required=False, maximum=None):
        if required or key in value:
            text = value.get(key)
            if not isinstance(text, str) or not text or (maximum and len(text) > maximum):
                raise ValueError(f"Invalid {key}")

    if kind == "connect":
        for key in ("voiceEnabled", "inputEnabled", "outputEnabled", "textOnly", "takeoverRequested"):
            if key in value and not isinstance(value[key], bool):
                raise ValueError(f"Invalid {key}")
        for key in (
            "provider",
            "outputVoice",
            "clientType",
            "clientLabel",
            "clientInstanceId",
            "timeZone",
            "locale",
            "workingDirectory",
        ):
            string(key, maximum=160 if key == "outputVoice" else None)
        if "clientStates" in value and (
            not isinstance(value["clientStates"], list)
            or any(not isinstance(item, str) or not item for item in value["clientStates"])
        ):
            raise ValueError("Invalid clientStates")
        if "inputCapabilities" in value:
            caps = value["inputCapabilities"]
            if not isinstance(caps, dict) or any(
                not isinstance(caps[key], bool)
                for key in ("text", "audio", "image", "resource")
                if key in caps
            ):
                raise ValueError("Invalid inputCapabilities")
    elif kind in {"audio.append", "input_audio_buffer.append"}:
        string("audio", required=True)
        if "being_name" in value:
            desktop_display_name(value["being_name"])
    elif kind in {"input.message", "text.message", "conversation.item.create"}:
        string("text")
        if "parts" in value:
            parts = value["parts"]
            if not isinstance(parts, list) or not 1 <= len(parts) <= 16:
                raise ValueError("Invalid parts")
            for part in parts:
                check("GatewayInputPartSchema", part)
                if part["type"] == "file" and "/" not in part["mime"]:
                    raise ValueError("Invalid MIME type")
        if "text" not in value and "parts" not in value:
            raise ValueError("A text or parts payload is required")
    elif kind in {"image.append", "input_image_buffer.append"}:
        string("image", required=True, maximum=256 * 1024)
        if value.get("media_type", "image/jpeg") != "image/jpeg":
            raise ValueError("Invalid media_type")
        if "occurred_at" in value and (type(value["occurred_at"]) is not int or value["occurred_at"] < 0):
            raise ValueError("Invalid occurred_at")
    elif kind in {"playback.started", "playback.ended", "playback.cancelled"}:
        string("responseId", required=True)
        if kind == "playback.cancelled" and "reason" in value and not isinstance(value["reason"], str):
            raise ValueError("Invalid reason")


def legacy_hello(value):
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise TypeError("Invalid legacy message")
    connection = {}
    for old, new in {
        "voiceEnabled": "voice_enabled",
        "inputEnabled": "input_enabled",
        "outputEnabled": "output_enabled",
        "textOnly": "text_only",
        "wakeWordOnly": "wake_word_only",
        "outputVoice": "output_voice",
        "workingDirectory": "working_directory",
        "clientStates": "client_states",
        "takeoverRequested": "takeover",
        "provider": "provider",
    }.items():
        if old in value:
            connection[new] = value[old]
    connection.setdefault("text_only", False)
    connection.setdefault("output_enabled", connection.get("voice_enabled", False))
    connection.setdefault("input_enabled", connection["output_enabled"])
    result = {
        "type": "session.hello",
        "event_id": uid(),
        "protocol": {"min": "7.0.0", "max": "7.0.0"},
        "client": {
            "type": value.get("clientType", "web"),
            "instance_id": value.get("clientInstanceId") or uid("legacy"),
        },
        "capabilities": [
            "input.text",
            "input.audio",
            "input.image",
            "input.file",
            "input.image_buffer",
            "playback.receipts",
            "client.events",
            "client.presence",
            "client.tools",
            "session.takeover",
        ],
        "connection": connection,
        "tools": value.get("clientTools", []),
        "locale": value.get("locale", "zh-CN"),
        "time_zone": value.get("timeZone", "Asia/Shanghai"),
    }
    hello(result)
    result["legacy"] = True
    return result


def valid_legacy(value):
    if not isinstance(value, dict):
        return False
    kind = value.get("type")
    try:
        business_event(value)
    except (ValueError, TypeError):
        return False
    if kind in {"client.action.result", "client.event.publish"}:
        try:
            event(value)
            return True
        except ValueError:
            return False
    if kind in {"audio.append", "playback.started", "playback.ended", "playback.cancelled"}:
        field = "audio" if kind == "audio.append" else "responseId"
        return isinstance(value.get(field), str) and bool(value[field])
    if kind in {"text.message", "input.message"}:
        return (isinstance(value.get("text"), str) and bool(value["text"])) or (
            isinstance(value.get("parts"), list) and 1 <= len(value["parts"]) <= 16
        )
    if kind == "connect":
        return all(
            isinstance(value[key], bool)
            for key in ("voiceEnabled", "inputEnabled", "outputEnabled", "textOnly", "takeoverRequested")
            if key in value
        )
    return kind in {
        "unmute",
        "mute",
        "input.unmute",
        "input.mute",
        "image.clear",
        "interrupt",
        "sleep",
        "wake",
        "input.suspend.ack",
    }
