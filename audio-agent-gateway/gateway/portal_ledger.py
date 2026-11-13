"""Portal's ledger.json, the same sub-agent status the desktop scene list shows."""

import json
import re
from pathlib import Path

STATUSES = {"queued", "running", "done", "failed", "cancelled", "interrupted", "budget_exhausted", "timeout"}
LABELS = {
    "queued": "等待执行",
    "running": "后台执行中",
    "done": "执行完成",
    "failed": "执行失败",
    "cancelled": "已取消",
    "interrupted": "已中断",
    "budget_exhausted": "达到任务限额",
    "timeout": "执行超时",
}
OPEN = {"queued", "running"}
MARKER = re.compile(r"<!-- gateway-task: (task_[a-f0-9]{32}) -->")


def read_ledger(directory):
    if not directory:
        return []
    file = Path(directory) / "ledger.json"
    try:
        if file.stat().st_size > 2_000_000:
            return []
        document = json.loads(file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    rows = document.get("tasks") if isinstance(document, dict) else None
    if not isinstance(rows, list):
        return []
    found = []
    for row in rows[-200:]:
        if not isinstance(row, dict):
            continue
        task_id, scene_id, status = row.get("task_id"), row.get("scene_id"), row.get("status")
        created = row.get("created_ms")
        if (not isinstance(task_id, str) or not task_id or not isinstance(scene_id, str) or not scene_id
                or status not in STATUSES or not isinstance(created, (int, float))):
            continue
        brief = row.get("brief_head") if isinstance(row.get("brief_head"), str) else ""
        marker = MARKER.search(brief)
        ended = row.get("ended_ms")
        found.append({
            "id": task_id[:256],
            "sceneId": scene_id,
            "status": status,
            "createdAt": created,
            "endedAt": ended if isinstance(ended, (int, float)) else None,
            "parentId": marker[1] if marker else None,
        })
    return found
