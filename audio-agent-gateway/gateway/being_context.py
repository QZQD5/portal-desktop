"""Acknowledged context changes shared by text and delegated Being requests."""

import hashlib
import json

from .conversation_context import context_text, stamp, task_summary


class BeingContext:
    def __init__(self, store, endpoint, *, tasks=None):
        self.store, self.endpoint = store, endpoint
        self.tasks = tasks if tasks is not None else {}
        with store.db:
            store.db.execute("""CREATE TABLE IF NOT EXISTS being_context (
                scope TEXT NOT NULL, item TEXT NOT NULL, sequence INTEGER NOT NULL, digest TEXT NOT NULL,
                PRIMARY KEY(scope, item))""")

    def capture(self, context):
        sequence = self.store.get("being_context_sequence", 0) + 1
        self.store.put("being_context_sequence", sequence)
        return stamp({**context, "capturedSequence": sequence})

    def received_task(self, task_id, scene_id, session_id):
        task = self.tasks.get(task_id, {})
        receipt = task.get("being", {})
        if (receipt.get("endpoint") == self.endpoint and receipt.get("scene_id") == scene_id
                and (session_id is None or receipt.get("session_id") == session_id)
                and (receipt.get("accepted") or receipt.get("stream_id") or receipt.get("stream_finished"))):
            return task
        return {}

    def prepare(self, scene_id, context, *, session_id=None):
        if not isinstance(session_id, str) or not session_id:
            session_id = None
        scope_parts = [self.endpoint, scene_id]
        if session_id:
            scope_parts.append(session_id)
        scope = hashlib.sha256(json.dumps(scope_parts).encode()).hexdigest()
        sequence = context.get("capturedSequence", 0)
        update = {key: context[key] for key in ("sessionId", "version", "capturedSequence", "currentRequest") if key in context}
        update["snapshot"] = self.store.db.execute(
            "SELECT 1 FROM being_context WHERE scope=? LIMIT 1", (scope,)).fetchone() is None
        update["messages"] = []
        entries = []
        current_task = (context.get("currentRequest") or {}).get("taskId")
        summaries, received_inputs = [], set()
        current = self.tasks.get(current_task, {})
        if current.get("turnId"):
            original = (current.get("priorObjectives") or [current["objective"]])[0]
            received_inputs.add((current["turnId"], original))
        for summary in context.get("tasks", []):
            task = self.received_task(summary.get("id"), scene_id, session_id)
            if task and task.get("turnId"):
                received_inputs.add((task["turnId"], task["objective"]))
            if ((current_task and summary.get("id") == current_task)
                    or (task and summary.get("objective") == task_summary(task)["objective"])):
                summary = {key: value for key, value in summary.items() if key != "objective"}
            summaries.append(summary)

        items = []
        for message in context.get("messages", []):
            source = message.get("source")
            same_session = session_id is None or message.get("beingSessionId") == session_id
            if same_session and (
                source == "legacy-desktop"
                or (source == "text-user" and message.get("deliveryStatus") == "accepted")
                or (source == "text-being" and message.get("from") not in (None, "partial"))
                or (source == "agent-result" and message.get("from") == "being")
            ):
                continue
            if (source in {"voice-user", "text-user"} and not message.get("truncated")
                    and (message.get("turnId"), message["content"]) in received_inputs):
                continue
            if source == "agent-result" and message.get("from") is None:
                task = self.received_task(message.get("taskId"), scene_id, session_id)
                if (task.get("status") == "completed" and task["being"].get("stream_finished")
                        and (task.get("result") or "").startswith(message["content"])):
                    continue
            items.append(("message:" + str(message["id"]), message))
        if "tasks" in context:
            items.append(("tasks", summaries))
        if "textWork" in context:
            items.append(("textWork", context["textWork"]))
        # Clear the old local memory background without altering Being's own memory.
        items.append(("memory", []))
        for key, value in items:
            digest = hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
            known = self.store.db.execute(
                "SELECT sequence,digest FROM being_context WHERE scope=? AND item=?", (scope, key)).fetchone()
            entries.append((scope, key, sequence, digest))
            if known and known[1] == digest:
                continue
            if key.startswith("message:"):
                update["messages"].append(value)
            else:
                update[key] = value
        # Text chat must not append a rules+JSON block when nothing Being-visible changed.
        # Task submission and voice session.update still send full context elsewhere.
        if not update.get("snapshot") and not update["messages"] and not any(
            key in update for key in ("tasks", "textWork", "memory")
        ):
            return "", entries
        return context_text(update), entries

    def acknowledge(self, entries):
        # Late acknowledgements must not roll back newer synchronization records.
        with self.store.db:
            self.store.db.executemany("""INSERT INTO being_context VALUES (?,?,?,?)
                ON CONFLICT(scope,item) DO UPDATE SET sequence=excluded.sequence,digest=excluded.digest
                WHERE excluded.sequence >= being_context.sequence""", entries)
