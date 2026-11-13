"""Bounded public execution records for voice tasks, using Heart's SSE facts."""
import json

from .logging import redact
from .store import now_ms


def update_activity(task, event, data, clean):
    entries = task.setdefault("activity", [])
    now = now_ms()
    if event == "reset":
        entries.clear()
        return
    if event in {"thinking", "reasoning"}:
        delta = data.get("text") or data.get("delta", "")
        if isinstance(delta, dict):
            delta = delta.get("text", "")
        if not isinstance(delta, str):
            return
        if not entries or entries[-1]["type"] != "think" or entries[-1].get("done"):
            entries.append({"type": "think", "id": f"think:{now}:{len(entries)}", "text": "", "ts": now})
        entry = entries[-1]
        entry["text"] = clean(entry["text"] + delta)[-2000:]
        entry["preview"] = entry["text"][:100]
    elif event in {"tool_use", "tool_result"}:
        call_id = next((data[k] for k in ("tool_call_id", "tool_use_id", "call_id", "id")
                        if isinstance(data.get(k), str) and data[k]), None)
        candidates = [e for e in entries if e["type"] == "tool" and
                      (e.get("toolCallId") == call_id if call_id else
                       not e.get("done") and (not data.get("name") or e.get("name") == data["name"]))]
        if event == "tool_use":
            if call_id and candidates:
                return
            for entry in entries:
                if entry["type"] == "think" and not entry.get("done"):
                    entry["done"] = True
            raw = data.get("input", "")
            entries.append({"type": "tool", "id": call_id or f"tool:{now}:{len(entries)}",
                            **({"toolCallId": call_id} if call_id else {}),
                            "name": clean(str(data.get("name") or "工具"))[:128], "ts": now,
                            "inputDetail": clean(raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False))[:1500]})
        else:
            if len(candidates) == 1 and not candidates[0].get("done"):
                entry = candidates[0]
            elif not call_id:
                # Legacy parallel calls can have the same name and no identity.
                # Show the returned material without guessing which call ended.
                entry = {"type": "tool-result", "id": f"result:{now}:{len(entries)}",
                         "name": "工具返回（关联待确认）", "ts": now}
                entries.append(entry)
            else:
                return
            result = data.get("result")
            metadata = {**(result if isinstance(result, dict) else {}), **data}
            entry.update(done=True, error=bool(metadata.get("is_error") or metadata.get("isError")
                                             or metadata.get("exit_code", 0)))
            raw = data.get("summary") or data.get("content") or result or data.get("stdout") or ""
            detail = clean(raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False))[:2000]
            entry.update(result=detail[:120], resultDetail=detail)
    elif event in {"message_stop", "error"}:
        for entry in entries:
            if entry["type"] == "think":
                entry["done"] = True
    task["lastActivityAt"] = now
    # Keep unresolved calls visible even when trimming old records.
    while len(entries) > 48 or len(json.dumps(entries, ensure_ascii=False).encode()) > 24000:
        index = next((i for i, entry in enumerate(entries) if entry.get("done")), 0)
        entries.pop(index)


def public_detail(value, secrets=()):
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    return redact(text)
