"""One event-loop-owned SQLite connection; writes are transactional and bounded."""

import json
import sqlite3
import time
from datetime import UTC, datetime
from urllib.parse import quote
from uuid import uuid4

from .errors import GatewayError
from .response_guards import RESERVED_ENVELOPE
from .retention import needs_recovery, public_event, select, task_of


def uid(prefix="event"):
    return f"{prefix}_{uuid4().hex}"


def now_ms():
    return int(time.time() * 1000)


def public_message(message):
    if not message.get("inputs"):
        return message
    path = "api/conversations/" + quote(message["sessionId"], safe="")
    path += "/messages/" + quote(message["id"], safe="") + "/inputs/"
    return {
        **message,
        "inputs": [
            {
                **part,
                "url": path + str(index) if part["url"].startswith("data:") else part["url"],
            }
            for index, part in enumerate(message["inputs"])
        ],
    }


class Store:
    def __init__(self, path):
        self.archive = None
        self.scene = {}
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.last_maintenance = 0
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY, session TEXT NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, session TEXT NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS replay_events (
                session TEXT NOT NULL, sequence INTEGER NOT NULL, body TEXT NOT NULL,
                PRIMARY KEY(session, sequence));
            CREATE TABLE IF NOT EXISTS streams (session TEXT PRIMARY KEY, sequence INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS events_session ON events(session, sequence);
            CREATE INDEX IF NOT EXISTS messages_session ON messages(session, id);
        """)
        if not self.get("replay_v2"):
            with self.db:
                self.db.execute(
                    "INSERT OR IGNORE INTO replay_events SELECT session, ROW_NUMBER() OVER (PARTITION BY session ORDER BY sequence), body FROM events"
                )
                self.db.execute(
                    "INSERT OR IGNORE INTO streams SELECT session, MAX(sequence) FROM replay_events GROUP BY session"
                )
            self.put("replay_v2", True)
        # Recover images from retained tasks written before messages stored attachments.
        with self.db:
            for task in self.get("tasks", {}).values():
                inputs = [p for p in task.get("parts", []) if p.get("mime", "").startswith("image/")]
                if inputs and task.get("turnId"):
                    self.db.execute(
                        """UPDATE messages SET body=json_set(body,'$.inputs',json(?))
                           WHERE session=? AND json_extract(body,'$.turnId')=?
                           AND json_extract(body,'$.role')='user'
                           AND json_extract(body,'$.inputs') IS NULL""",
                        (json.dumps(inputs), task["sessionId"], task["turnId"]),
                    )

    def attach_history(self, archive, scene):
        self.archive, self.scene = archive, scene
        for (body,) in self.db.execute("SELECT body FROM messages ORDER BY id"):
            archive.capture(json.loads(body), scene)

    def get(self, key, default=None):
        row = self.db.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, key, value):
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO kv VALUES (?, ?)", (key, json.dumps(value, ensure_ascii=False))
            )

    def delete(self, key):
        with self.db:
            self.db.execute("DELETE FROM kv WHERE key=?", (key,))

    def message(self, session, role, content, *, existing_only=False, **fields):
        if self.get("conversation_deleted:" + session):
            return None
        item = {
            "id": uid("message"),
            "role": role,
            "content": content,
            "sessionId": session,
            "createdAt": now_ms(),
            **self.scene,
            **fields,
        }
        with self.db:
            existing = self.db.execute(
                "SELECT id,body FROM messages WHERE session=? AND json_extract(body,'$.id')=? LIMIT 1",
                (session, item["id"]),
            ).fetchone()
            if existing_only and not existing:
                return None
            if existing:
                previous = json.loads(existing[1])
                item = {
                    **previous,
                    **item,
                    "createdAt": fields.get("createdAt", previous["createdAt"]),
                }
                for key in ("scene_id", "scene_meta"):
                    if key not in fields and key in previous:
                        item[key] = previous[key]
                self.db.execute(
                    "UPDATE messages SET body=? WHERE id=?",
                    (json.dumps(item, ensure_ascii=False), existing[0]),
                )
            else:
                self.db.execute(
                    "INSERT INTO messages(session,body) VALUES (?,?)",
                    (session, json.dumps(item, ensure_ascii=False)),
                )
            self.db.execute(
                "DELETE FROM messages WHERE session=? AND id NOT IN (SELECT id FROM messages WHERE session=? ORDER BY id DESC LIMIT 100)",
                (session, session),
            )
        if self.archive:
            scene = {"scene_id": item.get("scene_id", self.scene.get("scene_id", "")),
                     "scene_meta": item.get("scene_meta", self.scene.get("scene_meta", {}))}
            self.archive.capture(item, scene)
        return item

    def discard_task_result(self, task_id):
        with self.db:
            self.db.execute("""DELETE FROM messages WHERE json_extract(body,'$.role')='assistant'
                AND (json_extract(body,'$.taskId')=? OR EXISTS (
                    SELECT 1 FROM json_each(messages.body,'$.taskIds') WHERE value=?))""", (task_id, task_id))
        if self.archive:
            self.archive.discard_task_result(task_id)

    def delete_conversation(self, session):
        if self.archive:
            self.archive.delete_session(session)
        with self.db:
            for table in ("messages", "events", "replay_events", "streams"):
                self.db.execute(f"DELETE FROM {table} WHERE session=?", (session,))
            self.db.execute("DELETE FROM kv WHERE key=?", ("journal_discarded:" + session,))
            # Late playback acknowledgements must not recreate deleted history.
            self.db.execute(
                "INSERT OR REPLACE INTO kv VALUES (?, 'true')", ("conversation_deleted:" + session,)
            )

    def conversations(self):
        rows = self.db.execute(
            """SELECT m.session,
                COALESCE((SELECT json_extract(first.body,'$.content')
                    FROM messages first WHERE first.session=m.session
                    AND json_extract(first.body,'$.role')='user'
                    ORDER BY first.id LIMIT 1), ''),
                MAX(json_extract(m.body,'$.createdAt'))
                FROM messages m GROUP BY m.session
                ORDER BY MAX(m.id) DESC"""
        ).fetchall()
        return [
            {"sessionId": session, "title": " ".join(title.split())[:80], "updatedAt": updated}
            for session, title, updated in rows
        ]

    def history(self, session=None, limit=40, *, include_results=False):
        where, params = ("m.session=? AND ", [session]) if session else ("", [])
        rows = self.db.execute(
            f"""SELECT m.body FROM messages m WHERE {where} (? OR (
                COALESCE(json_extract(m.body,'$.source'),'') <> 'agent-result' OR NOT EXISTS (
                    SELECT 1 FROM messages p WHERE p.session=m.session
                    AND json_extract(p.body,'$.source')='agent-presentation'
                    AND (json_extract(p.body,'$.taskId')=json_extract(m.body,'$.taskId')
                        OR EXISTS (SELECT 1 FROM json_each(p.body,'$.taskIds') ids
                            WHERE ids.value=json_extract(m.body,'$.taskId')))
                ))) ORDER BY m.id DESC LIMIT ?""",
            (*params, include_results, limit),
        ).fetchall()
        return [public_message(json.loads(row[0])) for row in reversed(rows)]

    def message_input(self, session, message_id, index):
        row = self.db.execute(
            """SELECT json_extract(body, ?) FROM messages
               WHERE session=? AND json_extract(body,'$.id')=? LIMIT 1""",
            (f"$.inputs[{index}]", session, message_id),
        ).fetchone()
        if not row or not row[0]:
            raise KeyError("Message attachment not found")
        return json.loads(row[0])

    def event(self, session, event):
        item = {"event_id": uid(), "occurred_at": now_ms(), **event}
        with self.db:
            self.db.execute(
                "INSERT INTO events(session,body) VALUES (?,?)",
                (session, json.dumps(item, ensure_ascii=False)),
            )
            self.db.execute(
                "INSERT INTO streams VALUES (?,1) ON CONFLICT(session) DO UPDATE SET sequence=sequence+1",
                (session,),
            )
            sequence = self.db.execute("SELECT sequence FROM streams WHERE session=?", (session,)).fetchone()[
                0
            ]
            self.db.execute(
                "INSERT INTO replay_events VALUES (?,?,?)",
                (session, sequence, json.dumps(item, ensure_ascii=False)),
            )
            item["sequence"] = sequence
            self.db.execute(
                "DELETE FROM replay_events WHERE session=? AND sequence<=?", (session, sequence - 10000)
            )
            self.compact(session)
        return item

    def compact(self, session, *, max_events=2000, max_bytes=8 * 1024 * 1024):
        count, size = self.db.execute(
            "SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM events WHERE session=?",
            (session,),
        ).fetchone()
        if count <= max_events and size <= max_bytes:
            return
        rows = self.db.execute(
            "SELECT sequence,body FROM events WHERE session=? ORDER BY sequence", (session,)
        ).fetchall()
        retained = set(select(rows, max_events=max_events, max_bytes=max_bytes))
        self.db.executemany(
            "DELETE FROM events WHERE sequence=?", [(row[0],) for row in rows if row[0] not in retained]
        )
        key = "journal_discarded:" + session
        self.db.execute(
            "INSERT OR REPLACE INTO kv VALUES (?,?)",
            (key, json.dumps(self.get(key, 0) + len(rows) - len(retained))),
        )

    def maintain(
        self, protect=(), *, max_sessions=256, max_bytes=128 * 1024 * 1024, max_age_ms=30 * 86400000
    ):
        with self.db:
            rows = self.db.execute(
                "SELECT session,MAX(json_extract(body,'$.occurred_at')),SUM(length(CAST(body AS BLOB))) FROM events GROUP BY session ORDER BY 2"
            ).fetchall()
            count, size = len(rows), sum(row[2] for row in rows)
            for session, stamp, weight in rows:
                if session in protect or (
                    count <= max_sessions
                    and size <= max_bytes
                    and now_ms() - (stamp or now_ms()) <= max_age_ms
                ):
                    continue
                latest = {}
                for (body,) in self.db.execute(
                    "SELECT body FROM events WHERE session=? ORDER BY sequence", (session,)
                ):
                    task = task_of(json.loads(body))
                    if task and task.get("id"):
                        latest[task["id"]] = task
                if any(needs_recovery(task) for task in latest.values()):
                    continue
                for table in ("events", "replay_events", "messages", "streams"):
                    self.db.execute(f"DELETE FROM {table} WHERE session=?", (session,))
                self.db.execute("DELETE FROM kv WHERE key=?", ("journal_discarded:" + session,))
                count -= 1
                size -= weight
        self.last_maintenance = time.monotonic()

    def replay(self, session, after=0, limit=50):
        rows = self.db.execute(
            "SELECT sequence,body FROM replay_events WHERE session=? AND sequence>? ORDER BY sequence LIMIT ?",
            (session, after, limit + 1),
        ).fetchall()
        bounds = self.db.execute(
            "SELECT MIN(sequence),MAX(sequence) FROM replay_events WHERE session=?", (session,)
        ).fetchone()
        if after > (bounds[1] or 0):
            raise GatewayError("session_expired", "replay cursor is ahead of the current session")
        if bounds[0] and after < bounds[0] - 1:
            raise GatewayError("sequence_expired", "replay cursor is older than the retained event window")
        events, size = [], 0
        for seq, body in rows[:limit]:
            event = {**public_event(json.loads(body)), "sequence": seq}
            weight = len(json.dumps(event, ensure_ascii=False).encode())
            # One retained event must always advance the cursor, even if it fills a page.
            if events and size + weight > 4 * 1024 * 1024:
                break
            events.append(event)
            size += weight
        return {
            "events": events,
            "earliest_sequence": bounds[0] or 0,
            "latest_sequence": bounds[1] or 0,
            "next_sequence": events[-1]["sequence"] if events else after,
            "has_more": len(events) < len(rows),
        }

    def close(self):
        self.db.close()

    def delivery(self, session, state, **fields):
        item = {"type": "delivery." + state, "occurred_at": now_ms(), "event_id": uid(), "delivery": fields}
        with self.db:
            self.db.execute(
                "INSERT INTO events(session,body) VALUES (?,?)",
                (session, json.dumps(item, ensure_ascii=False)),
            )
            self.compact(session)

    def journal(self, session):
        records = [
            {**public_event(json.loads(body)), "sequence": sequence}
            for sequence, body in self.db.execute(
                "SELECT sequence,body FROM events WHERE session=? ORDER BY sequence", (session,)
            ).fetchall()
        ]
        stamp = lambda value: datetime.fromtimestamp(value / 1000, UTC).isoformat()
        header = {
            "schema": "qwaudio.session/v1",
            "type": "session",
            "version": 1,
            "sessionId": session,
            "createdAt": stamp(records[0]["occurred_at"]) if records else iso_now(),
        }
        if discarded := self.get("journal_discarded:" + session):
            header["retention"] = {"policy": "bounded", "discardedEvents": discarded}
        result = [header]
        for event in records:
            payload = {
                "schema": header["schema"],
                "sessionId": session,
                "seq": event["sequence"],
                "time": stamp(event["occurred_at"]),
                "source": "python-gateway",
            }
            if event["type"] == "journal.import":
                result.append({**event["record"], "seq": event["sequence"], "sessionId": session})
                continue
            if event["type"] == "transcript.final":
                if (
                    event.get("role") == "assistant"
                    and event.get("origin", "model") == "model"
                    and RESERVED_ENVELOPE.search(event.get("content", ""))
                ):
                    continue
                payload.update(
                    type=event["role"] + "/message",
                    turnId=event.get("turnId"),
                    payload={
                        "messageId": event.get("messageId", "user:" + event.get("turnId", "")),
                        "content": event["content"],
                        "taskIds": event.get("taskIds", []),
                        **({"inputs": event["inputs"]} if event.get("inputs") else {}),
                    },
                )
            elif event["type"].startswith("delivery."):
                payload.update(
                    type="qwaudio/delivery/event",
                    payload={"domainType": event["type"], **event.get("delivery", {})},
                )
            elif event.get("task"):
                payload.update(
                    type="qwaudio/task/event",
                    taskId=event["task"]["id"],
                    payload={"domainType": event["type"], "task": event["task"]},
                )
            else:
                continue
            result.append(payload)
        return result

    def session_replay(self, session):
        records = self.journal(session)
        messages, tasks, deliveries = {}, {}, []
        for event in records[1:]:
            payload = event["payload"]
            if event["type"] in {"user/message", "assistant/message"}:
                identity = payload.get("messageId") or f"journal:{event['seq']}"
                messages[identity] = {
                    "id": identity,
                    "role": event["type"].split("/")[0],
                    "source": event.get("source", "conversation-sync"),
                    "turnId": event.get("turnId"),
                    **payload,
                    "seq": event["seq"],
                    "time": event["time"],
                    "createdAt": int(datetime.fromisoformat(event["time"]).timestamp() * 1000),
                }
            elif event["type"] == "qwaudio/task/event":
                tasks[event["taskId"]] = {
                    **payload["task"],
                    "journalSeq": event["seq"],
                    "domainType": payload["domainType"],
                }
            elif event["type"] == "qwaudio/delivery/event":
                deliveries.append(event)
        return {
            "header": records[0],
            "lastSeq": records[-1].get("seq", 0),
            "messages": list(messages.values()),
            "tasks": list(tasks.values()),
            "deliveries": deliveries,
        }


def iso_now():
    return datetime.now(UTC).isoformat()
