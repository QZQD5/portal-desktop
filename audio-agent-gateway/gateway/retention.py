"""Compact durable facts before reclaiming old sessions; recovery state is never evicted."""

import json
import time
from urllib.parse import quote

from .errors import GatewayError


def public_task(task, *, detail=False):
    # Attachment bodies remain in task persistence for Being, never in events or status replies.
    result = dict(task)
    if task.get("startedAt") and task.get("status") in {"running", "delegated", "finalizing", "cancelling"}:
        result["elapsedMs"] = max(0, int(time.time() * 1000) - task["startedAt"])
    if "parts" in task:
        result["parts"] = [
            {key: part[key] for key in ("type", "mime", "filename", "source") if key in part}
            if part.get("type") == "file"
            else part
            for part in task["parts"]
        ]
    if detail or len(json.dumps(result, ensure_ascii=False).encode()) <= 32768:
        return result
    # Keep live events small; the authenticated task endpoint retains the complete result.
    result = {
        key: value
        for key, value in result.items()
        if key not in {"parts", "artifacts", "partialText", "delegation"}
    }
    for key in ("objective", "result", "error", "message", "fallbackText"):
        if isinstance(result.get(key), str):
            result[key] = result[key][:2000]
    result["artifacts"] = [
        {
            "artifactId": "task-details:" + task["id"],
            # shortcut: deferred bodies follow task retention; archive separately for longer access.
            "parts": [
                {
                    "url": "api/tasks/" + quote(task["id"], safe=""),
                    "mediaType": "application/vnd.gateway.task+json",
                }
            ],
        }
    ]
    return result


def public_event(event):
    event = dict(event)
    if event.get("task"):
        event["task"] = public_task(event["task"])
    if "tasks" in event:
        event["tasks"] = [public_task(task) for task in event["tasks"]]
    if event.get("type") == "journal.import":
        record = event["record"]
        event["record"] = {**record, "payload": public_event(record.get("payload", {}))}
    return event


def task_of(event):
    return event.get("task") or event.get("record", {}).get("payload", {}).get("task")


def needs_recovery(task):
    return task and (
        task.get("status") not in {"completed", "failed", "cancelled"}
        or task.get("notificationStatus") in {"pending", "delivering"}
    )


def select(rows, *, max_events=2000, max_bytes=8 * 1024 * 1024):
    selected, tasks, messages = {}, {}, {}
    used = 0

    def add(row, required=False):
        nonlocal used
        sequence, body = row
        if sequence in selected:
            return
        size = len(body.encode())
        if len(selected) >= max_events or used + size > max_bytes:
            if required:
                raise GatewayError(
                    "SESSION_JOURNAL_CAPACITY", "Session recovery state exceeds journal budget"
                )
            return
        selected[sequence] = row
        used += size

    if not rows:
        return []
    for row in rows:
        event = json.loads(row[1])
        task = task_of(event)
        if task and task.get("id"):
            tasks.pop(task["id"], None)
            tasks[task["id"]] = row
        record = event.get("record", {})
        if event["type"] == "transcript.final" or record.get("type") in {"user/message", "assistant/message"}:
            key = event.get("messageId") or record.get("payload", {}).get("messageId") or row[0]
            messages.pop(key, None)
            messages[key] = row
    add(rows[-1], True)
    for row in tasks.values():
        if needs_recovery(task_of(json.loads(row[1]))):
            add(row, True)
    for row in list(messages.values())[-200:][::-1]:
        add(row)
    terminal = [row for row in tasks.values() if not needs_recovery(task_of(json.loads(row[1])))]
    for row in terminal[-100:][::-1]:
        add(row)
    for row in reversed(rows):
        if len(selected) >= int(max_events * 0.75) or used >= int(max_bytes * 0.75):
            break
        event = json.loads(row[1])
        if (
            task_of(event)
            or event["type"] == "transcript.final"
            or event.get("record", {}).get("type") in {"user/message", "assistant/message"}
        ):
            continue
        add(row)
    return sorted(selected)
