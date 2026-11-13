import asyncio
import contextlib
import copy
import logging
import time

from .being import MEMORY_WRITE_KIND, BeingClient
from .content import normalize_parts
from .conversation_context import context_text, snapshot, stamp, task_in_scene
from .portal_ledger import LABELS, OPEN, read_ledger
from .recurrence import next_occurrence
from .retention import public_task
from .store import now_ms, uid
from .subagent_tasks import TERMINAL as CHILD_TERMINAL
from .subagent_tasks import pending
from .task_activity import public_detail, update_activity

log = logging.getLogger("gateway.tasks")
TERMINAL = {"completed", "failed", "cancelled"}
STATES = {"queued": "submitted", "scheduled": "submitted", "running": "working",
          "recovering": "working", "delegated": "working", "finalizing": "working",
          "cancelling": "working", "completed": "completed", "failed": "failed", "cancelled": "cancelled"}


class TaskProgress:
    def __init__(self, manager, task):
        self.manager, self.task = manager, task

    async def __call__(self, message):
        await self.manager.report_progress(self.task, message)

    async def activity(self, event, data):
        await self.manager.report_activity(self.task, event, data)


class TaskManager:
    def __init__(self, settings, store, emit):
        self.settings, self.store, self.emit = settings, store, emit
        self.tasks = store.get("tasks", {})
        self.notification_sequence = max((t.get("notificationSequence", 0) for t in self.tasks.values()), default=0)
        self.client = BeingClient(settings, store=store, tasks=self.tasks)
        self.worker = self.scheduler = self.active = None
        self.session_model = "Being"
        self.overdue, self.cancellations = {}, {}
        self.session_grants, self.task_grants = {}, set()
        self.events = asyncio.Queue()
        self.stopping = False
        self.activity_sent = {}
        self.update_locks = {}
        self.capacity = asyncio.Semaphore(settings.max_concurrent_tasks)
        self.owner_capacity = asyncio.Semaphore(1)
        self.execution = None
        for task in self.tasks.values():
            if task["status"] not in TERMINAL | {"scheduled", "delegated"}:
                if task.get("being", {}).get("submitted"):
                    task["status"] = "recovering"
                elif task["status"] != "queued":
                    task.update(status="failed", error="旧任务未自动重发，请核对原任务结果。", completedAt=now_ms())
            if task.get("notificationStatus") == "delivering":
                task["notificationStatus"] = "pending"
        self.persist()

    def persist(self):
        self.prune()
        self.store.put("tasks", self.tasks)


    def prune(self):
        now = now_ms()
        retained = []
        removed = set()
        for key, task in self.tasks.items():
            if task["status"] not in TERMINAL:
                continue
            pending = task.get("notificationStatus") in {"pending", "delivering"}
            ttl = self.settings.pending_notification_ttl if pending else self.settings.terminal_task_ttl
            if now - (task.get("completedAt") or task["createdAt"]) >= ttl * 1000:
                removed.add(key)
            elif not pending:
                retained.append(task)
        retained.sort(key=lambda task: task["createdAt"], reverse=True)
        removed.update(task["id"] for task in retained[self.settings.max_terminal_tasks :])
        for key in removed:
            task = self.tasks.pop(key)
            if task.get("discard_output") or task["status"] == "cancelled":
                scene_id = (task.get("scene") or {}).get("scene_id")
                self.store.put("discarded_task:" + key, {"scene_id": scene_id})
                for child in task.get("being", {}).get("subagents", {}):
                    self.store.put("discarded_child:" + child, {"id": key, "scene_id": scene_id})
            self.store.delete("task_context:" + key)
            self.task_grants.discard(key)
        self.session_grants = {key: at for key, at in self.session_grants.items() if at > time.monotonic()}

    def output_discarded(self, task_id):
        if not isinstance(task_id, str):
            return False
        if task_id in self.tasks:
            task = self.tasks[task_id]
            return bool(task.get("discard_output") or task.get("status") == "cancelled")
        return bool(self.store.get("discarded_task:" + task_id))

    def callback_owner(self, source_id, scene_id):
        if not isinstance(source_id, str) or len(source_id) > 256:
            return None
        if source_id.startswith("task_"):
            task = self.tasks.get(source_id)
            if task:
                owner = {"id": source_id, "scene_id": (task.get("scene") or {}).get("scene_id")}
            else:
                owner = self.store.get("discarded_task:" + source_id)
                if owner:
                    owner["id"] = source_id
        else:
            owner = next(({"id": task["id"], "scene_id": (task.get("scene") or {}).get("scene_id")}
                          for task in self.tasks.values()
                          if source_id in task.get("being", {}).get("subagents", {})), None)
            owner = owner or self.store.get("discarded_child:" + source_id)
        if owner and (scene_id is None or owner["scene_id"] == scene_id):
            return owner["id"]

    async def start(self):
        self.worker = asyncio.create_task(self.run_queue())
        overdue = sorted(
            (
                task
                for task in self.tasks.values()
                if task["status"] == "scheduled" and task["schedule"]["executeAt"] <= now_ms()
            ),
            key=lambda task: task["schedule"]["executeAt"],
        )
        self.overdue = {
            task["id"]: time.monotonic() + index * self.settings.reminder_stagger
            for index, task in enumerate(overdue)
        }
        self.scheduler = asyncio.create_task(self.run_schedules())
        self.ledger_checked = 0


    async def run_schedules(self):
        while not self.stopping:
            try:
                await self.tick_schedules()
                await self.reconcile_portal_ledger()
                self.prune()
            except Exception:
                log.exception("task.schedule_error")
            await asyncio.sleep(0.2)


    async def reconcile_portal_ledger(self):
        # Same ledger.json the desktop scene list renders. A missing tool result must not keep a finished child running.
        now = time.monotonic()
        if now - getattr(self, "ledger_checked", 0) < 1:
            return
        self.ledger_checked = now
        rows = read_ledger(self.settings.portal_state_dir)
        if not rows:
            return
        grouped = {}
        for row in rows:
            parent = row["parentId"] if row["parentId"] in self.tasks else None
            if not parent and not row["parentId"]:
                scene_rows = [task for task in self.tasks.values()
                              if (task.get("scene") or {}).get("scene_id") == row["sceneId"]
                              and task["status"] in {"delegated", "running"}
                              and not (task.get("being") or {}).get("subagents")
                              and "spawn" in ((task.get("being") or {}).get("subagentCalls") or {}).values()
                              and row["createdAt"] >= task.get("createdAt", 0)]
                parent = scene_rows[0]["id"] if len(scene_rows) == 1 else None
            if parent:
                grouped.setdefault(parent, []).append(row)
        for task_id, children in grouped.items():
            task = self.tasks.get(task_id)
            if not task or task.get("discard_output") or task["status"] in TERMINAL:
                continue
            receipt = task.setdefault("being", {})
            known = receipt.setdefault("subagents", {})
            previous = dict(known)
            for child in children:
                known[child["id"]] = child["status"]
            if known:
                calls = receipt.get("subagentCalls") or {}
                receipt["subagentCalls"] = {key: value for key, value in calls.items() if value != "spawn"}
            signature = [[key, value] for key, value in sorted(known.items())]
            if signature == task.get("portalLedger"):
                continue
            task["portalLedger"] = signature
            if any(status in OPEN for status in known.values()):
                labels = "、".join(f"{LABELS.get(child['status'], child['status'])} {child['id']}" for child in children)
                task["message"] = labels
                if known != previous:
                    await self.publish(task, "task.updated")
                continue
            if all(status == "done" for status in known.values()):
                task.update(status="completed", result=task.get("result") or "执行完成。",
                            error=None, resultSource="portal-ledger")
            elif all(status == "cancelled" for status in known.values()):
                task.update(status="cancelled", result=None, error=None, notificationStatus="none")
            else:
                failed = next(child for child in children if child["status"] not in {"done", "cancelled"})
                task.update(status="failed", result=None, error=f"{LABELS[failed['status']]} {failed['id']}",
                            resultSource="portal-ledger")
            task.update(completedAt=now_ms(),
                        notificationStatus="pending" if task["status"] != "cancelled" and task.get("kind") != MEMORY_WRITE_KIND else "none")
            await self.publish(task)


    def list(self, session=None, active=False, limit=100):
        tasks = [
            t
            for t in self.tasks.values()
            if (not session or t["sessionId"] == session) and (not active or t["status"] not in TERMINAL)
        ]
        return [
            public_task(task) for task in sorted(tasks, key=lambda t: t["createdAt"], reverse=True)[:limit]
        ]


    async def publish(self, task, event=None, **details):
        if task.get("announcements_suppressed") and task.get("notificationStatus") in {"pending", "delivering"}:
            task["notificationStatus"] = "none"
        if task.get("notificationStatus") == "pending" and "notificationSequence" not in task:
            self.notification_sequence += 1
            task.update(notificationSequence=self.notification_sequence, notificationQueuedAt=now_ms())
        if (task["status"] in {"completed", "failed"} and (task.get("result") or task.get("error"))
                and task.get("resultSource") != "portal-callback"):
            receipt = task.get("being", {})
            provenance = ({"from": "being", "beingSessionId": receipt.get("session_id")}
                          if task["status"] == "completed" and receipt.get("stream_finished") else {})
            self.store.message(
                task["sessionId"],
                "assistant",
                task.get("result") or task["error"],
                id="agent:" + task["id"],
                source="agent-result",
                voicePending=task.get("notificationStatus") == "pending" and not task.get("chatStreamId"),
                taskId=task["id"],
                turnId=task.get("turnId"),
                contextVersion=task.get("contextVersion"),
                **{**task.get("scene", {}), **provenance},
                createdAt=task.get("completedAt") or now_ms(),
            )
        task["workState"] = (
            "auth_required"
            if task.get("authorization", {}) and task["authorization"]["status"] == "pending"
            else "input_required"
            if task.get("inputRequest", {}) and task["inputRequest"]["status"] == "pending"
            else STATES[task["status"]]
        )
        task["elapsedMs"] = max(
            0, (task.get("completedAt") or now_ms()) - (task.get("startedAt") or now_ms())
        )
        self.persist()
        try:
            await self.emit(
                task["sessionId"],
                copy.deepcopy(
                    {"type": event or f"task.{task['status']}", "task": public_task(task), **details}
                ),
            )
        except Exception:
            log.exception("task.observer_failed id=%s", task["id"])


    async def submit(
        self,
        objective,
        session,
        turn=None,
        *,
        schedule=None,
        parts=None,
        submission_key=None,
        kind=None,
        request_context=None,
        scene=None,
        chat_stream_id=None,
    ):
        parts = normalize_parts(parts) if parts else []
        if not isinstance(objective, str) or not objective.strip() or len(objective) > 32000:
            raise ValueError("Task objective is empty or too long")
        if schedule:
            schedule = {
                **schedule,
                "at": schedule["executeAt"],
                "recurrenceStartAt": schedule.get("recurrenceStartAt", schedule["executeAt"]),
            }
        if submission_key:
            existing = next(
                (t for t in self.tasks.values() if t.get("submissionKey") == submission_key), None
            )
            if existing:
                return existing
        task = {
            "scene": copy.deepcopy(self.settings.being_scene if scene is None else scene),
            "id": uid("task"),
            "status": "scheduled" if schedule else "queued",
            "scope": "user",
            "workState": "submitted",
            "kind": kind
            or (("reminder" if schedule["type"] == "reminder" else "scheduled_task") if schedule else "work"),
            "objective": objective.strip(),
            "ownerId": self.settings.owner_id,
            "sessionId": session,
            "turnId": turn,
            "createdAt": now_ms(),
            "startedAt": None,
            "completedAt": None,
            "elapsedMs": 0,
            "result": None,
            "error": None,
            "artifacts": [],
            "activity": [],
            "authorization": None,
            "inputRequest": None,
            "notificationStatus": "none",
            "schedule": schedule,
            "parts": parts or [],
        }
        if schedule and schedule.get("seriesId"):
            task["seriesId"] = schedule["seriesId"]
        if submission_key:
            task["submissionKey"] = submission_key
        if chat_stream_id:
            task["chatStreamId"] = chat_stream_id
        self.tasks[task["id"]] = task
        context = (
            copy.deepcopy(request_context)
            if request_context is not None
            else snapshot(self.store, self.tasks, session)
        )
        context["currentRequest"] = {"taskId": task["id"], "turnId": turn, "createdAt": task["createdAt"]}
        self.store.put("task_context:" + task["id"], self.client.context.capture(context))
        await self.publish(task, "task.scheduled" if schedule else "task.accepted")
        self.events.put_nowait(True)
        return task


    async def update(self, task_id, objective, session, turn, user_text="", *, scene_id=None):
        if not isinstance(objective, str) or not objective.strip() or len(objective) > 32000:
            raise ValueError("Task objective is empty or too long")
        objective = objective.strip()
        scope = self.settings.being_scene.get("scene_id") if scene_id is None else scene_id
        async with self.update_locks.setdefault(task_id, asyncio.Lock()):
            task = self.tasks.get(task_id)
            if (not task or not task_in_scene(task, scope, session)
                    or (scene_id is None and task["sessionId"] != session)):
                raise ValueError("只能修正当前场景中的工作。")
            if task.get("discard_output"):
                return {"status": "discarded", "task_id": task_id}
            if task["status"] in TERMINAL | {"scheduled", "cancelling"}:
                raise ValueError("该任务已结束、正在停止或尚未到执行时间，不能修正执行中的目标。")
            previous = task.get("lastUpdate", {})
            if previous.get("turnId") == turn:
                return {"status": previous["status"], "task_id": task_id,
                        "user_message": "本轮修正已经发送或正在核对，不要重复提交。"}
            if previous.get("status") == "unknown":
                raise ValueError("上次修正的发送结果尚待核对，不能自动重发。")
            submitted = task.get("being", {}).get("submitted")
            if submitted:
                # Persist uncertainty before network I/O; a lost response is not permission to resend.
                task["lastUpdate"] = {"turnId": turn, "objective": objective, "status": "unknown"}
                self.persist()
                try:
                    await self.client.update(task, objective, user_text)
                except Exception as exc:
                    if task.get("discard_output"):
                        return {"status": "discarded", "task_id": task_id}
                    if isinstance(exc, ValueError):
                        if previous:
                            task["lastUpdate"] = previous
                        else:
                            task.pop("lastUpdate", None)  # Validation failed before any POST.
                        self.persist()
                    raise
                if task.get("discard_output"):
                    return {"status": "discarded", "task_id": task_id}
            elif objective == user_text:
                task.setdefault("priorObjectives", []).append(task["objective"])
            task["objective"] = objective
            status = "accepted" if submitted else "updated"
            task["lastUpdate"] = {"turnId": turn, "objective": objective, "status": status,
                                  "userText": user_text}
            await self.publish(task, "task.updated")
            if task.get("discard_output"):
                return {"status": "discarded", "task_id": task_id}
            return {"status": status, "task_id": task_id, "task": public_task(task),
                    "user_message": "修正已发送，等待执行方按新要求继续，尚不能确认已切换。" if submitted
                    else "尚未提交的任务目标已更新，将按修正后的要求执行。"}

    async def run_queue(self):
        while not self.stopping:
            try:
                task = next(
                    (t for t in self.tasks.values() if t["status"] in {"queued", "recovering"}),
                    None,
                )
                if task:
                    async with self.owner_capacity, self.capacity:
                        if task["status"] in {"queued", "recovering"}:
                            await self.execute(task)
                else:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self.events.get(), 0.5)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("task.queue_error")
                await asyncio.sleep(0.5)


    async def report_progress(self, task, message):
        if not task.get("discard_output"):
            task["message"] = message

    async def report_activity(self, task, event, data):
        if task.get("discard_output"):
            return
        update_activity(task, event, data, lambda text: public_detail(
            text, (self.settings.being_token, self.settings.doubao_api_key)))
        now = time.monotonic()
        if event in {"thinking", "reasoning"} and now - self.activity_sent.get(task["id"], 0) < 0.25:
            return
        self.activity_sent[task["id"]] = now
        self.persist()
        # Public task snapshots make reconnects idempotent; no model call is involved.
        await self.publish(task, "task.activity")


    def start_cancel(self, task_id, *, discard_output=True):
        task = self.tasks[task_id]
        if discard_output and not self.output_discarded(task_id):
            task.update(discard_output=True, result=None, error=None, notificationStatus="none")
            task.pop("partialText", None)
            if task.get("lastUpdate", {}).get("status") == "unknown":
                task.pop("lastUpdate")
            self.persist()
            self.store.discard_task_result(task_id)
        operation = self.cancellations.get(task_id)
        if operation is None:
            operation = asyncio.create_task(self._cancel(task_id))
            self.cancellations[task_id] = operation

            def completed(job):
                self.cancellations.pop(task_id, None)
                if not job.cancelled():
                    job.exception()

            operation.add_done_callback(completed)
        return operation


    async def cancel(self, task_id, *, discard_output=True):
        return await asyncio.shield(self.start_cancel(task_id, discard_output=discard_output))


    async def tick_schedules(self):
        if not self.settings.reminder_scheduler:
            return
        for task in list(self.tasks.values()):
            schedule = task.get("schedule")
            if task["status"] != "scheduled" or schedule["executeAt"] > now_ms():
                continue
            if time.monotonic() < self.overdue.get(task["id"], 0):
                continue
            self.overdue.pop(task["id"], None)
            task["status"] = "completed" if task["kind"] == "reminder" else "queued"
            if task["kind"] == "reminder":
                task.update(
                    result=task["objective"],
                    startedAt=now_ms(),
                    completedAt=now_ms(),
                    notificationStatus="pending",
                )
            await self.publish(task, "task.completed" if task["kind"] == "reminder" else "task.accepted")
            recurrence = schedule.get("recurrence", "once")
            next_at = next_occurrence(
                schedule.get("recurrenceStartAt", schedule["executeAt"]),
                recurrence,
                now_ms(),
                schedule.get("timeZone", "UTC"),
            )
            if next_at:
                await self.submit(
                    task["objective"],
                    task["sessionId"],
                    task.get("turnId"),
                    schedule={**schedule, "executeAt": next_at},
                    request_context=self.store.get(
                        "task_context:" + task["id"],
                        {
                            "sessionId": task["sessionId"],
                            "messages": [],
                            "tasks": [],
                            "historyUnavailable": True,
                        },
                    ),
                )


    async def execute(self, task):
        self.active = task
        task.update(status="running", startedAt=task.get("startedAt") or now_ms(), message="正在联系 Being。")
        await self.publish(task)
        context = self.store.get("task_context:" + task["id"], {})
        context.pop("memory", None)
        task["contextVersion"] = stamp(context)["version"]
        self.store.put("task_context:" + task["id"], context)
        self.execution = asyncio.create_task(self.client.run(task, context_text(context) if context else "",
                                                              self.persist, TaskProgress(self, task),
                                                              lambda text: self.report_output(task, text)))
        try:
            try:
                result = await self.execution
            finally:
                # SSE can close before the stop receipt; do not publish a result or start the next task yet.
                if task["status"] == "cancelling" and (operation := self.cancellations.get(task["id"])):
                    with contextlib.suppress(Exception):
                        await asyncio.shield(operation)
            if task["status"] != "cancelled":
                if pending(task.get("being", {})):
                    task.update(status="delegated", partialText=None if task.get("discard_output") else result, result=None,
                                message="Being 已委派子代理；最终回复将由后台回调送达，子任务结束状态尚待核对。")
                elif not result and not task.get("discard_output"):
                    raise RuntimeError("Being 未返回可播报的结果，请在聊天中查看任务进展。")
                else:
                    task.update(status="completed", result=None if task.get("discard_output") else result)
        except asyncio.CancelledError:
            if task["status"] == "cancelled":
                pass
            elif self.stopping:
                task["status"] = "recovering" if task.get("being", {}).get("submitted") else "queued"
            else:
                raise
        except Exception as exc:
            # Error strings must never expose a credential-bearing URL.
            if task["status"] != "cancelled":
                error = str(exc).replace(self.settings.being_token, "[redacted]")
                task.update(status="failed", error=None if task.get("discard_output") else error)
        finally:
            if task["status"] in TERMINAL:
                notify = task["status"] != "cancelled" and task.get("kind") != MEMORY_WRITE_KIND and not task.get("discard_output")
                task.update(completedAt=now_ms(), notificationStatus="pending" if notify else "none")
            await self.publish(task)
            self.active = self.execution = None
            self.activity_sent.pop(task["id"], None)

    async def complete_subagents(self, children, text, task_ids):
        for task_id in task_ids:
            task = self.tasks.get(task_id)
            if not task or task["status"] != "delegated" or task.get("discard_output"):
                continue
            receipt = task.get("being", {})
            known = receipt.get("subagents", {})
            for child in known.keys() & children.keys():
                known[child] = children[child]
            if not known or pending(receipt) or not all(status in CHILD_TERMINAL for status in known.values()):
                self.persist()
                continue
            status = ("completed" if all(value == "done" for value in known.values()) else
                      "cancelled" if all(value == "cancelled" for value in known.values()) else "failed")
            task.update(status=status, result=text if status == "completed" and not task.get("discard_output") else None,
                        error=text if status == "failed" and not task.get("discard_output") else None, completedAt=now_ms(),
                        notificationStatus="none", resultSource="portal-callback")
            await self.publish(task)

    async def report_output(self, task, text):
        if task.get("discard_output"):
            return
        try:
            await self.emit(task["sessionId"], {"type": "task.output.delta", "taskId": task["id"],
                             "content": text, **task.get("scene", {}),
                             **({"chatStreamId": task["chatStreamId"]} if task.get("chatStreamId") else {})})
        except Exception:
            log.exception("task.output_observer_failed id=%s", task["id"])

    def pending_permissions(self, session=None):
        return []

    async def respond_permission(self, *args, **kwargs):
        raise ValueError("请通过 Portal Desktop 的工具授权窗口操作。")

    async def respond_input(self, *args, **kwargs):
        raise ValueError("请在 Being 聊天中补充任务信息。")

    async def _cancel(self, task_id):
        task = self.tasks[task_id]
        if task["status"] in TERMINAL:
            if task.get("discard_output"):
                await self.publish(task, "task.output.discarded")
            return task
        if self.active is task or pending(task.get("being", {})):
            previous = task["status"]
            task["status"] = "cancelling"
            await self.publish(task)
            try:
                await self.client.cancel(task)
            except Exception:
                if task["status"] == "cancelling":
                    task["status"] = previous
                    await self.publish(task)
                raise
            if task["status"] in TERMINAL:
                return task
            task["status"] = "cancelled"
            if self.active is task and self.execution:
                self.execution.cancel()
                await asyncio.gather(self.execution, return_exceptions=True)
        task.update(status="cancelled", completedAt=now_ms(), notificationStatus="none")
        await self.publish(task)
        return task

    async def close(self):
        self.stopping = True
        for job in (self.scheduler, self.worker):
            if job:
                job.cancel()
        await asyncio.gather(*(job for job in (self.scheduler, self.worker) if job), return_exceptions=True)
        await asyncio.gather(*self.cancellations.values(), return_exceptions=True)
        self.persist()
        await self.client.close()
