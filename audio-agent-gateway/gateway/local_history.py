"""Gateway-owned local history. Transport journals may expire; this archive does not."""
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path


class LocalHistory:
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS history (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, identity TEXT UNIQUE NOT NULL,
                scene TEXT NOT NULL, body TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS history_scene ON history(scene, seq);
            CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS chat_events (seq INTEGER PRIMARY KEY, body TEXT NOT NULL);
        """)

    def put(self, identity, message):
        content = message.get("content")
        if not isinstance(content, str):
            raise ValueError("History content must be text")
        body = {**message}
        body.setdefault("id", identity)
        body.pop("seq", None)
        body.setdefault("at", datetime.now(UTC).isoformat())
        body.setdefault("scene_id", "")
        with self.db:
            self.db.execute("""INSERT INTO history(identity,scene,body) VALUES(?,?,?)
                ON CONFLICT(identity) DO UPDATE SET scene=excluded.scene,body=excluded.body""",
                (identity, body["scene_id"], json.dumps(body, ensure_ascii=False)))
            seq = self.db.execute("SELECT seq FROM history WHERE identity=?", (identity,)).fetchone()[0]
        return {**body, "seq": seq}

    def capture(self, message, scene):
        if self.state("deleted-session:" + message["sessionId"]):
            return
        if message.get("unconfirmed") or message.get("source") == "agent-presentation":
            return
        if message.get("partialTranscript") and message.get("playbackStatus") == "started":
            return
        body = {**message, "scene_id": message.get("scene_id", scene.get("scene_id", "")),
                "scene_label": message.get("scene_meta", scene.get("scene_meta", {})).get("scene_label", "")}
        if message.get("source") == "voice-assistant":
            body["voiceKey"] = message["id"]
            body["voicePending"] = message.get("playbackStatus") in {"not_started", "started"}
        if isinstance(message.get("createdAt"), (int, float)):
            body["at"] = datetime.fromtimestamp(message["createdAt"] / 1000, UTC).isoformat()
        identity = "voice:" + message["sessionId"] + ":" + message["id"]
        if message.get("source") == "agent-result" and message.get("taskId"):
            identity = "task-result:" + message["taskId"]
            body["voiceKey"] = "agent:" + message["taskId"]
            body.setdefault("sourceSessionId", message["sessionId"])
        return self.put(identity, body)

    def find(self, identity):
        row = self.db.execute("SELECT seq,body FROM history WHERE identity=?", (identity,)).fetchone()
        return {**json.loads(row[1]), "seq": row[0]} if row else None

    def page(self, *, after=None, before=None, scene=None, limit=100, include_callbacks=False):
        clauses, args = [], []
        for name, operator, value in (("seq", ">", after), ("seq", "<", before), ("scene", "=", scene)):
            if value is not None:
                clauses.append("(scene=? OR (scene='' AND json_extract(body,'$.source')='portal-callback'))"
                               if name == "scene" and include_callbacks else f"{name}{operator}?")
                args.append(value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        order = "ASC" if after is not None else "DESC"
        rows = self.db.execute(f"SELECT seq,body FROM history{where} ORDER BY seq {order} LIMIT ?",
                               [*args, min(500, max(1, limit))]).fetchall()
        return sorted(({**json.loads(body), "seq": seq} for seq, body in rows), key=lambda row: row["seq"])

    def last_seq(self):
        return self.db.execute("SELECT COALESCE(MAX(seq),0) FROM history").fetchone()[0]

    def scenes(self):
        result = []
        for scene, count, at in self.db.execute("""SELECT scene,COUNT(*),MAX(json_extract(body,'$.at'))
                FROM history WHERE scene<>'' GROUP BY scene"""):
            row = self.page(scene=scene, limit=1)[0]
            result.append({"sceneId": scene, "messageCount": count, "lastActive": at,
                           "label": row.get("scene_label", "")})
        return result

    def import_legacy(self, messages, *, complete=False):
        if not isinstance(messages, list) or len(messages) > 500:
            raise ValueError("Import at most 500 history records")
        for row in messages:
            if (not isinstance(row, dict) or type(row.get("seq")) is not int or row["seq"] <= 0
                    or not isinstance(row.get("content"), str) or len(row["content"]) > 1_000_000
                    or row.get("role") not in {"user", "assistant", "being", "system"}
                    or not isinstance(row.get("scene_id", ""), str)):
                raise ValueError("Invalid legacy history record")
        for row in messages:
            identity = "heart:" + str(row["seq"])
            if not self.db.execute("SELECT 1 FROM history WHERE identity=?", (identity,)).fetchone():
                self.put(identity, {**row, "source": "legacy-desktop", "upstream_seq": row["seq"]})
        if complete:
            self.state("legacy-desktop-import-complete", True)
        return self.state("legacy-desktop-import-complete") is True

    def discard_task_result(self, task_id):
        with self.db:
            self.db.execute("""DELETE FROM history WHERE identity=?
                OR (json_extract(body,'$.role')='assistant' AND json_extract(body,'$.taskId')=?)""",
                ("task-result:" + task_id, task_id))

    def delete_session(self, session):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO state VALUES (?, 'true')", ("deleted-session:" + session,))
            self.db.execute("DELETE FROM history WHERE json_extract(body,'$.sessionId')=? OR json_extract(body,'$.sourceSessionId')=?",
                            (session, session))

    def save_chat(self, current, event=None, *, reset=False):
        # Cursor, accumulated text and replay events survive a process exit together.
        with self.db:
            if reset:
                self.db.execute("DELETE FROM chat_events")
                self.db.execute("INSERT OR REPLACE INTO state VALUES (?,?)", (
                    "request:" + current["request_id"], json.dumps({"stream_id": current["stream_id"]})))
            if event:
                self.db.execute("INSERT OR REPLACE INTO chat_events VALUES (?,?)", (event["seq"], json.dumps(event)))
                self.db.execute("DELETE FROM chat_events WHERE seq<=?", (event["seq"] - 10000,))
            self.db.execute("INSERT OR REPLACE INTO state VALUES ('chat',?)", (json.dumps(current),))

    def chat_events(self):
        return [json.loads(row[0]) for row in self.db.execute("SELECT body FROM chat_events ORDER BY seq")]

    def import_gateway(self, path, scene):
        path = Path(path)
        if not path.is_file():
            return
        # Read the original with WAL support; never remove or update a legacy database.
        source = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            for (body,) in source.execute("SELECT body FROM messages ORDER BY id"):
                self.capture(json.loads(body), scene)
        finally:
            source.close()

    def state(self, key, value=None):
        if value is not None:
            with self.db:
                self.db.execute("INSERT OR REPLACE INTO state VALUES(?,?)", (key, json.dumps(value)))
        row = self.db.execute("SELECT body FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def close(self):
        self.db.close()
