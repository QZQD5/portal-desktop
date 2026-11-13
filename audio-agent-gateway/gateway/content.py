"""Bounded Gateway input parts and attachment validation, independent of the speech provider."""

import base64
import copy
import hashlib
import json
import re
import time
from urllib.parse import urlsplit

REF = "audio-agent-gateway/inputRef"


def embedded(url):
    match = re.fullmatch(r"data:([^;,]+)(?:;charset=([^;,]+))?;base64,([a-z\d+/=\s]+)", url, re.IGNORECASE)
    if not match:
        return None
    data = re.sub(r"\s", "", match[3])
    try:
        raw = base64.b64decode(data, validate=True)
    except ValueError:
        raise ValueError("Invalid base64 attachment") from None
    return match[1].lower(), data, raw


def normalize_parts(parts):
    if not isinstance(parts, list) or not 1 <= len(parts) <= 16:
        raise ValueError("Input requires 1-16 parts")
    result, size = [], 0
    for part in parts:
        if not isinstance(part, dict):
            continue
        if part.get("type") == "text":
            text = str(part.get("text", "")).replace("\0", "")[:100000].strip()
            if text:
                result.append({"type": "text", "text": text})
            continue
        if part.get("type") != "file":
            raise ValueError("Unsupported input part")
        mime = str(part.get("mime") or part.get("mimeType") or "").replace("\0", "")[:160].strip().lower()
        url = str(part.get("url", "")).replace("\0", "")[: 20 * 1024 * 1024].strip()
        parsed = urlsplit(url)
        if (
            "/" not in mime
            or parsed.scheme not in {"data", "http", "https"}
            or parsed.scheme in {"http", "https"}
            and not parsed.hostname
        ):
            raise ValueError("Invalid attachment MIME or URL")
        data = embedded(url) if parsed.scheme == "data" else None
        if parsed.scheme == "data" and (not data or data[0] != mime):
            raise ValueError("Attachment data URL and MIME must match")
        if data:
            if len(data[2]) > 8 * 1024 * 1024:
                raise ValueError("Attachment exceeds 8 MB")
            size += len(data[2])
        normalized = {"type": "file", "mime": mime, "url": url}
        if part.get("filename"):
            normalized["filename"] = re.sub(r"[\r\n\t]", " ", str(part["filename"]))[:240].strip()
        source = part.get("source")
        if isinstance(source, dict):
            normalized["source"] = {
                "type": source.get("type")
                if source.get("type") in {"clipboard", "file", "resource"}
                else "file"
            }
            if path := str(source.get("path") or "").replace("\0", "")[:2048].strip():
                normalized["source"]["path"] = path
            text = source.get("text")
            if isinstance(text, dict) and text.get("value"):
                normalized["source"]["text"] = {
                    "value": str(text["value"]).replace("\0", "")[:2048],
                    **{key: text[key] for key in ("start", "end") if type(text.get(key)) is int},
                }
        result.append(normalized)
    if not result or size > 12 * 1024 * 1024:
        raise ValueError("Empty input or total attachments exceed 12 MB")
    return result


def anchored_parts(parts):
    files = [part for part in parts if part["type"] == "file"]
    if not files:
        return parts
    text = "\n".join(part["text"] for part in parts if part["type"] == "text")
    references = []
    for index, part in enumerate(files):
        label = part.get("source", {}).get("text", {}).get("value") or (
            f"[Image {index + 1}]"
            if part["mime"].startswith("image/")
            else "@" + part["filename"]
            if part.get("filename")
            else f"[File {index + 1}]"
        )
        ordinal = index + 1
        while label in references:
            label = f"[{'Image' if part['mime'].startswith('image/') else 'File'} {ordinal}]"
            ordinal += 1
        references.append(label)
    text = " ".join(filter(None, (" ".join(ref for ref in references if ref not in text), text)))
    for part, ref in zip(files, references, strict=True):
        start = text.index(ref)
        part["source"] = {
            **part.get("source", {}),
            "type": part.get("source", {}).get("type", "file"),
            "text": {"value": ref, "start": start, "end": start + len(ref)},
        }
    return [{"type": "text", "text": text}, *files]


def frontend_projection(parts):
    text = "\n".join(part["text"] for part in parts if part["type"] == "text")
    files = [part for part in parts if part["type"] == "file"]
    if not files:
        return text
    metadata = [
        {
            "type": "file",
            "mime": part["mime"],
            **({"id": part["_meta"][REF]} if part.get("_meta", {}).get(REF) else {}),
            **({"filename": part["filename"]} if part.get("filename") else {}),
            **({"source": {"type": part["source"]["type"], "text": {"value": part["source"]["text"]["value"]}}}
               if part.get("source", {}).get("text") else {}),
        }
        for part in files
    ]
    encoded = (
        json.dumps(metadata, ensure_ascii=False)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )
    return text + "\n\n<input_parts>\n" + encoded + "\n</input_parts>"


class InputAssets:
    def __init__(self, ttl=21600, limit=500):
        self.sessions = {}
        self.ttl, self.limit = ttl, limit

    def state(self, session_id, create=True):
        now = time.monotonic()
        self.sessions = {
            key: value for key, value in self.sessions.items() if now - value["access"] < self.ttl
        }
        if session_id not in self.sessions and create:
            if len(self.sessions) >= self.limit:
                del self.sessions[min(self.sessions, key=lambda key: self.sessions[key]["access"])]
            self.sessions[session_id] = {"assets": {}, "next": 1, "access": now}
        state = self.sessions.get(session_id)
        if state:
            state["access"] = now
        return state

    def register(self, session_id, parts):
        state = self.state(session_id)
        result = copy.deepcopy(parts)
        for part in result:
            if part["type"] != "file":
                continue
            fingerprint = hashlib.sha256((part["mime"] + "\0" + part["url"]).encode()).hexdigest()
            ref = next((key for key, item in state["assets"].items() if item[0] == fingerprint), None)
            if not ref:
                ref = f"input_{state['next']}"
                state["next"] += 1
                data = embedded(part["url"])
                state["assets"][ref] = (fingerprint, copy.deepcopy(part), len(data[2]) if data else 0)
                while (
                    len(state["assets"]) > 32
                    or sum(x[2] for x in state["assets"].values()) > 64 * 1024 * 1024
                ):
                    del state["assets"][next(iter(state["assets"]))]
            part["_meta"] = {REF: ref}
        return result

    def resolve(self, session_id, refs):
        if not refs:
            return []
        state = self.state(session_id, create=False)
        if not state or any(ref not in state["assets"] for ref in refs):
            raise ValueError("Input reference expired or belongs to another session")
        return [copy.deepcopy(state["assets"][ref][1]) for ref in dict.fromkeys(refs)]
