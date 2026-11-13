"""Seeduplex wire format, ported from the original Node Gateway's Doubao adapter.

Each upstream connection owns one instance: response IDs must not cross reconnects.
"""

import base64
import json
import math
import re
import struct
from urllib.parse import quote, urlsplit

from .store import uid

MODEL = "1.2.6.1"
VOICE = "zh_female_vv_jupiter_bigtts"
URL = "wss://openspeech.bytedance.com/api/v3/duplex/realtime/dialogue"
# Being Voice proxy. It checks the Loom token, then forwards Seeduplex JSON events.
VOICE_PROXY = "wss://101.132.86.87/v1/realtime/"
MODEL_PROFILE = {
    "id": MODEL,
    "label": "Doubao Seeduplex 3.0 Realtime",
    "family": "doubao-seeduplex",
    "sessionDefaults": {"voice": VOICE, "turnDetection": {"type": "server_vad"}},
    "modelCapabilities": {
        "textInput": True,
        "audioInput": True,
        "imageInput": False,
        "videoInput": False,
        "textOutput": True,
        "audioOutput": True,
        "functionCalling": True,
    },
    "transportCapabilities": {
        "textInput": True,
        "audioInput": True,
        "imageInput": False,
        "imageBufferInput": False,
    },
}


def pcm16(audio):
    raw = base64.b64decode(audio, validate=True)
    if len(raw) % 4:
        raise ValueError("Doubao Seeduplex returned invalid Float32 PCM audio")
    output = bytearray()
    for (sample,) in struct.iter_unpack("<f", raw):
        if not math.isfinite(sample):
            raise ValueError("Doubao Seeduplex returned non-finite PCM audio")
        sample = max(-1, min(1, sample))
        # Math.round in the reference adapter rounds negative ties toward +infinity.
        output.extend(struct.pack("<h", math.floor(sample * (32768 if sample < 0 else 32767) + 0.5)))
    return base64.b64encode(output).decode()


def classify_error(message):
    if re.search(r"no session active|no active session", message, re.IGNORECASE):
        return "no_active_session"
    if re.search(r"no active response|no response.*cancel", message, re.IGNORECASE):
        return "no_active_response"
    if re.search(
        r"invalid.*api.*key|authentication|unauthorized|forbidden|\b(?:401|403)\b|quota|arrearage",
        message,
        re.IGNORECASE,
    ):
        return "fatal"
    if re.search(
        r"idle|inactivity|timeout.*audio|session was closed because no response was generated for \d+ seconds",
        message,
        re.IGNORECASE,
    ):
        return "inactivity"
    if re.search(r"content|audit|safety|risk|inspection", message, re.IGNORECASE):
        return "content_safety"
    return "other"


def uses_voice_proxy(settings):
    return settings.doubao_url.rstrip("/") == URL.rstrip("/")


def voice_configured(settings):
    """Default voice goes through the Being proxy with the Loom token; direct Doubao is test-only."""
    if not uses_voice_proxy(settings):
        return bool(settings.doubao_api_key)
    return bool(settings.being_token)


def provider_connect(settings):
    """Voice uses the Being proxy and the Loom token. A non-default URL is only the in-process test upstream."""
    if not uses_voice_proxy(settings):
        if not settings.doubao_api_key:
            raise ValueError("测试上游需要 DOUBAO_API_KEY")
        return settings.doubao_url, {"X-Api-Key": settings.doubao_api_key}, 10
    being_id = urlsplit(settings.being_endpoint).path.strip("/")
    token = settings.being_token.strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", being_id):
        raise ValueError("当前 Being 链接不能用于语音代理")
    if not re.fullmatch(r"[A-Za-z0-9._~-]{16,2048}", token):
        raise ValueError("当前 Loom 凭据不能用于语音代理")
    return VOICE_PROXY + quote(being_id, safe=""), {"Authorization": "Bearer " + token}, 35


def session_config(settings, instructions, tools, voice=None):
    return {
        "model": settings.doubao_model,
        "instructions": instructions,
        "audio": {
            "input": {"format": {"type": "pcm", "rate": 16000}},
            "output": {
                "format": {"type": "pcm", "rate": 24000},
                "voice": voice or settings.doubao_voice,
                "speed": 0,
                "loudness": 0,
            },
        },
        "tools": [
            {
                "type": "function",
                **{
                    k: v
                    for k, v in tool.get("function", tool).items()
                    if k in {"name", "description", "parameters"}
                },
            }
            for tool in tools
            if tool.get("function", tool).get("name")
        ],
    }


def conversation_item(item):
    if item.get("type") == "function_call_output":
        output = item["output"]
        if isinstance(output, str):
            try:
                output = json.loads(output)
            except ValueError:
                pass
        if not isinstance(output, str):
            output = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
        normalized = {
            "role": "tool",
            "call_id": item["call_id"],
            "content": [{"type": "input_text", "text": output}],
        }
    else:
        text = "\n".join(
            str(p.get("text", "")).strip() for p in item.get("content", []) if str(p.get("text", "")).strip()
        )
        if not text:
            return None
        normalized = {
            "id": item.get("id") or uid("item"),
            "type": "message",
            "role": item.get("role", "user"),
            "content": [{"type": "input_text", "text": text}],
        }
    return {"type": "conversation.item.create", "items": [normalized]}


def conversation_history(messages):
    """Only completed user/assistant pairs are accepted by ConversationCreate."""
    items, pending = [], None
    for message in messages:
        if message["role"] == "user":
            pending = message
        elif message["role"] == "assistant" and pending:
            for entry in (pending, message):
                items.extend(
                    conversation_item(
                        {
                            "type": "message",
                            "role": entry["role"],
                            "content": [{"type": "input_text", "text": entry["content"]}],
                        }
                    )["items"]
                )
            pending = None
    return {"type": "conversation.item.create", "items": items} if items else None


class SeeduplexProtocol:
    def __init__(self):
        self.active = None
        self.wire_id = None
        self.finished = {}

    def finish_active(self):
        if self.active:
            self.finished[self.active] = None
            if self.wire_id:
                self.finished[self.wire_id] = None
            while len(self.finished) > 512:
                del self.finished[next(iter(self.finished))]
        self.active = None
        self.wire_id = None

    def normalize(self, event):
        if not isinstance(event, dict):
            raise TypeError("Invalid Seeduplex event")
        kind = event.get("type", "")
        if kind == "conversation.item.input_audio_transcription.started":
            return [{**event, "type": "input_audio_buffer.speech_started", "native_asr": True}]
        if kind == "conversation.item.input_audio_transcription.delta":
            # Native ASR deltas are replacement snapshots, unlike assistant text deltas.
            return [{**event, "text": event.get("delta", event.get("text", ""))}]
        if kind in {
            "conversation.item.input_audio_transcription.completed",
            "conversation.item.input_audio_transcription.failed",
        }:
            return [
                {**event, "type": "input_audio_buffer.speech_stopped"},
                {**event, "transcript": event.get("text", event.get("transcript", ""))},
            ]
        # UsageResponse has no response identity and can arrive after a newer reply starts.
        if kind == "response.done" and "usage" in (event.get("response") or {}):
            return [{**event, "type": "response.usage"}]
        if kind == "response.canceled":
            event = {
                **event,
                "type": "response.done",
                "response": {**(event.get("response") or {}), "status": "cancelled"},
            }
            kind = "response.done"
        if kind not in {
            "response.created",
            "response.output_text.delta",
            "response.output_text.done",
            "response.output_audio.started",
            "response.output_audio.delta",
            "response.output_audio.done",
            "response.function_call_arguments.done",
            "response.done",
        }:
            return [event]
        rid = event.get("response_id") or (event.get("response") or {}).get("id")
        # Late identified packets from a cancelled response cannot acquire the new response slot.
        if rid in self.finished:
            return []
        events = []
        if not self.active:
            self.active = rid or uid("response")
            self.wire_id = rid
            events.append({"type": "response.created", "response": {"id": self.active}})
        if rid and self.wire_id and rid != self.wire_id:
            raise ValueError("Overlapping Seeduplex responses")
        if rid:
            # FunctionCallResponse may omit IDs; retain its local ID when TTS supplies one.
            self.wire_id = rid
        rid = self.active
        if kind == "response.created":
            return events
        common = {**event, "response_id": rid}
        if kind == "response.output_text.delta":
            events.append(
                {
                    **common,
                    "type": "response.text.delta",
                    "delta": event.get("delta") or event.get("text", ""),
                }
            )
        elif kind == "response.output_text.done":
            events.append(
                {
                    **common,
                    "type": "response.text.done",
                    "text": event.get("text") or event.get("transcript", ""),
                }
            )
        elif kind == "response.output_audio.delta":
            events.append(
                {**common, "delta": pcm16(event.get("delta") or event.get("audio", "")), "sampleRate": 24000}
            )
        elif kind == "response.function_call_arguments.done":
            items = event.get("items", [event])
            call_ids = [item["call_id"] for item in items if item.get("call_id")]
            for item in items:
                events.append(
                    {
                        "type": kind,
                        "response_id": rid,
                        "call_id": item.get("call_id") or event.get("call_id", ""),
                        "name": item.get("name") or event.get("name", ""),
                        "arguments": item.get("arguments") or event.get("arguments", "{}"),
                        **({"batch_call_ids": call_ids} if len(call_ids) > 1 else {}),
                    }
                )
        elif kind in {"response.done", "response.output_audio.done"}:
            events.append(
                {
                    **common,
                    "type": "response.done",
                    "response": {"status": "completed", **(event.get("response") or {}), "id": rid},
                }
            )
            self.finish_active()
        elif kind == "response.output_audio.started":
            events.append({"type": "response.activity", "response_id": rid})
        else:
            events.append(common)
        return events
