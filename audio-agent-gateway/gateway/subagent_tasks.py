"""Track only child receipts associated with observed Portal tool calls."""
import json
import re

TERMINAL = {"done", "failed", "cancelled", "interrupted", "budget_exhausted", "timeout"}


def payload(value):
    for _ in range(4):
        if isinstance(value, str) and len(value) <= 2_000_000:
            try:
                value = json.loads(value)
            except ValueError:
                compact = re.search(r'"summary"\s*:\s*"(sub_[A-Za-z0-9_-]{1,128}) (queued|running|done|failed|cancelled|interrupted|budget_exhausted|timeout)(?:"|$)', value)
                if compact:
                    return {"task_id": compact[1], "status": compact[2]}
                decoder = json.JSONDecoder()
                for match in list(re.finditer(r"\{", value))[:16]:
                    try:
                        candidate, _ = decoder.raw_decode(value, match.start())
                    except ValueError:
                        continue
                    if isinstance(candidate, dict) and candidate.keys() & {"task_id", "tasks", "task", "content", "isError"}:
                        value = candidate
                        break
                else:
                    return None
        elif isinstance(value, list) or (isinstance(value, dict) and isinstance(value.get("content"), list)):
            if isinstance(value, dict) and (value.get("isError") or value.get("is_error")):
                return value
            texts = [item.get("text") for item in (value if isinstance(value, list) else value["content"])
                     if isinstance(item, dict) and isinstance(item.get("text"), str)]
            value = texts[0] if len(texts) == 1 else None
        else:
            return value if isinstance(value, dict) else None
    return None


def observe(receipt, event, data):
    identity = next((data[key] for key in ("tool_call_id", "tool_use_id", "call_id", "id")
                     if isinstance(data.get(key), str) and 0 < len(data[key]) <= 256), None)
    calls = receipt.setdefault("subagentCalls", {})
    if event == "tool_use":
        name = str(data.get("name", ""))
        match = re.search(r"(?:^|[:./_])portal_subagent_(spawn|status|control)$", name)
        if name == "act":
            arguments = payload(data.get("input")) or {}
            body = arguments.get("body", "")
            if isinstance(body, str):
                match = re.match(r"\s*portal_subagent_(spawn|status|control)\b(?!\s*\?)", body)
        operation = match[1] if match else "other"
        if len(calls) >= 128:
            if operation == "spawn":
                calls["overflow"] = "spawn"
        else:
            if not identity:
                index = 0
                while f"unidentified:{index}" in calls:
                    index += 1
                identity = f"unidentified:{index}"
            calls[identity] = operation
        return
    if event != "tool_result":
        return
    if not identity and calls and len(set(calls.values())) == 1:
        # Legacy act events have no call IDs; same-operation receipts are unordered.
        identity = next((key for key in calls if key.startswith("unidentified:")), None)
    if not identity or identity not in calls:
        return
    if calls[identity] == "other":
        calls.pop(identity)
        return
    metadata = {**(data["result"] if isinstance(data.get("result"), dict) else {}), **data}
    if metadata.get("is_error") or metadata.get("isError") or metadata.get("exit_code", 0):
        calls.pop(identity)
        return
    value = next((decoded for field in ("result", "content", "stdout", "summary")
                  if (decoded := payload(data.get(field)))), None)
    if not value:
        return
    if value.get("isError") or value.get("is_error"):
        calls.pop(identity)
        return
    operation = calls[identity]
    rows = value.get("tasks", [value.get("task", value)]) if operation == "status" else [value]
    children = receipt.setdefault("subagents", {})
    recorded = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        child = row.get("task_id")
        if (isinstance(child, str) and re.fullmatch(r"sub_[A-Za-z0-9_-]{1,128}", child)
                and (operation == "spawn" or child in children)):
            status = row.get("status")
            children[child] = status if isinstance(status, str) and status in TERMINAL | {"queued", "running"} else "unknown"
            recorded.append(child)
    if operation != "spawn" or recorded:
        calls.pop(identity)
    return recorded


def pending(receipt):
    return (any(operation == "spawn" for operation in receipt.get("subagentCalls", {}).values())
            or any(status not in TERMINAL for status in receipt.get("subagents", {}).values()))


def require_stopped(receipt):
    # shortcut: Heart has no child cancellation API; require an actual Portal receipt.
    if pending(receipt):
        raise ValueError("Being 已派出子代理，尚未确认子任务停止。请在 Being 聊天中取消对应子任务后再核对；本次不会报告全部已取消。")
