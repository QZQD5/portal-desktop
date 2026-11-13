"""Local text admission, durable history and replay; UI disconnects never stop Being."""
import asyncio
import json
import logging
import re
from urllib.parse import unquote
from uuid import uuid4

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .errors import GatewayError
from .subagent_tasks import observe, pending
from .tasks import TERMINAL

log = logging.getLogger("gateway.chat")
CALLBACK_MARKER = re.compile(r"\A\s*<!-- gateway-task: (task_[a-f0-9]{32}) -->[ \t]*(?:\r?\n|$)")


class Chat:
    def __init__(self, client, history, before_send=None, context_changed=None, tasks=None):
        self.client, self.history = client, history
        self.before_send = before_send
        self.context_changed = context_changed
        self.tasks = tasks
        self.gateway = None
        self.current = history.state("chat")
        self.events = history.chat_events()
        self.changed = asyncio.Event()
        self.admission = asyncio.Lock()
        self.job = None
        self.readers = {}
        self.voice_stops = {}
        self.request_order = (self.current or {}).get("order", 0)
        self.callback_observer = None
        self.callback_discovery = asyncio.Lock()

    def start(self):
        if self.current and not self.current.get("finished"):
            if self.current.get("routed"):
                # Old local-router streams: only wait for an already submitted task.
                if self.current.get("task_id"):
                    self.start_run()
                else:
                    self.current["finished"] = True
                    self.save()
            else:
                self.start_run(self.current.get("body"), recover=self.current.get("submitted", False))
        self.callback_observer = asyncio.create_task(self.watch_callbacks())

    async def watch_callbacks(self):
        # shortcut: only Heart's latest stream is replayable; add callback lookup to recover replaced streams.
        while True:
            await asyncio.sleep(2)
            try:
                await self.discover_callback()
            except Exception:
                # Request exceptions may contain the Heart token. Keep logs metadata-only.
                log.warning("callback discovery failed; will retry")

    async def discover_callback(self):
        async with self.callback_discovery:
            previous, order = self.current, self.request_order
            response = await self.client.http.get("api/stream/active", timeout=15)
            self.client.check(response)
            if response.status_code == 204:
                return
            active = response.json()
            if (not isinstance(active, dict) or active.get("origin") != "callback"
                    or not isinstance(active.get("stream_id"), str) or not 1 <= len(active["stream_id"]) <= 256
                    or type(active.get("finished")) is not bool or not isinstance(active.get("events"), list)):
                return
            checkpoint = self.history.state("callback:" + active["stream_id"])
            if checkpoint and checkpoint.get("finished"):
                return
            if checkpoint and checkpoint.get("stream_id") in self.readers:
                return
            async with self.admission:
                foreground = (self.current is previous and self.request_order == order
                              and (not previous or previous.get("finished")))
                shared = {"scene_id": None, "scene_meta": {}}
                scenes = {"": shared}
                known = [self.client.settings.being_scene]
                known += [entry["scene"] for entry in self.client.settings.legacy_history]
                known += [{"scene_id": row["sceneId"], "scene_meta": {"scene_label": row["label"]}}
                          for row in self.history.scenes()]
                for scene in known:
                    if isinstance(scene.get("scene_id"), str) and scene["scene_id"]:
                        scenes[scene["scene_id"]] = scene
                current = checkpoint or self.new_stream(shared, "callback:" + active["stream_id"], order)
                if self.tasks:
                    children = current.setdefault("subagents", {})
                    for task in self.tasks.tasks.values():
                        if task["status"] == "delegated" or self.tasks.output_discarded(task["id"]):
                            for child, status in task.get("being", {}).get("subagents", {}).items():
                                children.setdefault(child, status)
                current.update(callback=True, upstream_id=active["stream_id"], scenes=scenes,
                               finished=False, meta={**current.get("meta", {}), "origin": "callback"})
                current.setdefault("stream_scene", active.get("scene_id"))
                events = []
                if foreground:
                    self.current, self.events = current, events
                    self.history.save_chat(current, reset=True)
                self.save(current)
                self.start_run(current=current, events=events, recover=True, snapshot=active)
                self.changed.set()

    def start_run(self, body=None, *, current=None, events=None, **options):
        current = self.current if current is None else current
        events = self.events if events is None else events
        if current.get("routed"):
            job = asyncio.create_task(self.run_routed(body, current, events, options.get("context")))
        else:
            job = asyncio.create_task(self.run(body, current=current, events=events, **options))
        self.readers[current["stream_id"]] = job
        if current is self.current:
            self.job = job
        def finished(done):
            if self.readers.get(current["stream_id"]) is done:
                self.readers.pop(current["stream_id"])
        job.add_done_callback(finished)

    def routed_task(self, current):
        if task_id := current.get("task_id"):
            return self.tasks.tasks.get(task_id)
        return next((task for task in self.tasks.tasks.values()
                     if task.get("turnId") == current["turn_id"]
                     and task["scene"].get("scene_id") == current["scene"]["scene_id"]), None)

    def routed_delta(self, text, current, events):
        before = current["text"]
        if text.startswith(before) and len(text) > len(before):
            current["text"] = text
            self.emit("content_block_delta", {"delta": {"text": text[len(before):]}}, current=current, events=events)

    async def run_routed(self, body, current, events, context):
        # Recover an in-flight task from the retired local text router.
        gateway = self.gateway
        queue = asyncio.Queue(maxsize=100)
        gateway.subscribers.add(queue)
        try:
            task = self.routed_task(current)
            if not task:
                raise GatewayError("text_route_unconfirmed", "未能确认原文字请求是否已委派。", 409)
            queue.task_id = task["id"]
            while not task.get("discard_output") and task["status"] not in TERMINAL | {"delegated"}:
                event = await queue.get()
                if (not task.get("discard_output") and event.get("taskId") == task["id"]
                        and event["type"] == "task.output.delta"):
                    self.routed_delta(event["content"], current, events)
            if task["status"] == "cancelled":
                current["stopped"] = True
                self.emit("stream_stopped", {}, current=current, events=events)
            elif task.get("discard_output"):
                current["discard_output"] = True
                self.emit("stream_discarded", {}, current=current, events=events)
            elif task["status"] == "failed":
                raise GatewayError("being_task_failed", task.get("error") or "Being 任务执行失败。", 502)
            else:
                text = task.get("result") or task.get("partialText")
                if text:
                    self.routed_delta(text, current, events)
                    saved = self.history.find("task-result:" + task["id"])
                    persisted = ({
                        "kind": "self", "seq": saved["seq"], "stream_id": current["stream_id"], "from": "final",
                    } if saved else self.archive_reply(current=current))
                    self.emit("message_stop", {"persisted": persisted} if persisted else {},
                              current=current, events=events)
        except GatewayError as exc:
            current["failed"] = True
            self.emit("error", {"message": str(exc)}, current=current, events=events)
        except Exception:
            current["failed"] = True
            log.warning("chat.text_route_failed")
            self.emit("error", {"message": "文字请求未能完整结束。"}, current=current, events=events)
        finally:
            gateway.subscribers.discard(queue)
            if not asyncio.current_task().cancelling():
                current["finished"] = True
                self.save(current)
                self.changed.set()
                await self.sync_context(current)

    @staticmethod
    def new_stream(scene, request_id, order):
        return {"stream_id": "local-" + uuid4().hex, "scene": scene, "request_id": request_id,
                "sequence": 0, "upstream_seq": 0, "text": "", "reply": 0, "finished": False,
                "submitted": True, "order": order}

    def save(self, current=None):
        current = self.current if current is None else current
        if current.get("callback"):
            self.history.state("callback:" + current["upstream_id"],
                               {"finished": True} if current.get("finished") else current)
        if current is self.current:
            self.history.save_chat(current)

    async def sync_context(self, current, feedback=None, *, scene=None):
        if current.get("discard_output") or self.callback_discarded(current):
            feedback = None
        if (self.tasks and feedback and feedback.get("final") and feedback.get("text")
                and current.get("callback") and not current.get("failed")):
            await self.tasks.complete_subagents(current.get("subagents", {}), feedback["text"],
                                                current.get("callback_task_ids", ()))
        if self.context_changed:
            await self.context_changed((scene or current.get("reply_scene", current["scene"]))["scene_id"],
                                       {"stream_id": current["stream_id"], **feedback,
                                        "task_ids": current.get("callback_task_ids", [])} if feedback else None)

    def emit(self, event, data, *, current=None, events=None):
        current = self.current if current is None else current
        events = self.events if events is None else events
        scene = current.get("reply_scene", current["scene"])
        data = {**data, "stream_id": current["stream_id"], **scene}
        task_id = next(iter(current.get("callback_task_ids", ())), None) or current.get("task_id")
        if task_id:
            data["taskId"] = task_id
        current["sequence"] += 1
        item = {"seq": current["sequence"], "event": event, "data": data}
        if task_id or current.get("routed"):
            item["task_id"] = task_id
        events.append(item)
        del events[:-10000]
        if current is self.current:
            self.history.save_chat(current, item)
        elif not current.get("routed"):
            self.archive_reply(partial=True, current=current)
        if current.get("callback"):
            self.history.state("callback:" + current["upstream_id"], current)
        self.changed.set()

    def archive_reply(self, *, partial=False, source="final", session_id=None, current=None):
        current = self.current if current is None else current
        if current.get("discard_output") or self.callback_discarded(current) or not current["text"]:
            return None
        scene = current.get("reply_scene", current["scene"])
        task_id = next(iter(current.get("callback_task_ids", ())), None) or current.get("task_id")
        row = self.history.put(f"text:{current['stream_id']}:{current['reply']}", {
            "role": "assistant", "content": current["text"],
            "source": "portal-callback" if current.get("callback") else "text-being",
            "from": "partial" if partial else source, "scene_id": scene["scene_id"] or "",
            "scene_label": scene.get("scene_meta", {}).get("scene_label", ""),
            **({"taskId": task_id} if task_id else {}),
            **({"beingSessionId": session_id} if isinstance(session_id, str) and session_id else {}),
        })
        return {"kind": "self", "seq": row["seq"], "stream_id": current["stream_id"],
                "from": "partial" if partial else source}

    def consume(self, event, data, sequence=None, *, current=None, events=None):
        if not isinstance(data, dict):
            return
        data = dict(data)
        current = self.current if current is None else current
        counted = event != "meta" or data.get("continuation") is True or data.get("persisted")
        current["upstream_seq"] = sequence if sequence is not None else current.get("upstream_seq", 0) + int(bool(counted))
        if event == "meta" and data.get("stream_id"):
            if current.get("upstream_id") and current["upstream_id"] != data["stream_id"]:
                raise RuntimeError("Stream identity changed")
            current["upstream_id"] = data["stream_id"]
        if "scene_id" in data:
            current["stream_scene"] = data["scene_id"]
        # Human streams accept submitted scenes; callbacks also accept the shared SBS scene.
        scenes = current.get("scenes", {current["scene"]["scene_id"]: current["scene"]})
        scene = current.get("callback_scene") or scenes.get(current.get("stream_scene", current["scene"]["scene_id"]) or "")
        if self.callback_discarded(current):
            current["text"] = ""
            self.save(current)
            return
        if current.get("discard_output"):
            if scene is not None and event in {"tool_use", "tool_result"}:
                observe(current, event, data)
            self.save(current)
            return
        if scene != current.get("reply_scene", current["scene"]):
            saved = self.archive_reply(partial=True, current=current)
            if saved:
                self.emit("message_stop", {"persisted": saved}, current=current, events=events)
                current["reply"] += 1
                current["text"] = ""
        if scene is None:
            self.save(current)
            return
        current["reply_scene"] = scene
        if event in {"tool_use", "tool_result"}:
            observe(current, event, data)
        feedback = None
        persisted = data.pop("persisted", None)
        if event == "error":
            if "LLM stream stalled: no data received for" in str(data.get("message", "")):
                data["message"] = self.client.error_message(data)
            current["failed"] = True
            feedback = {"text": "Being 处理这条文字请求时遇到错误，请查看聊天中的详情。", "final": True}
        if event in {"tool_use", "tool_result"}:
            current["progress"] = "Being 正在执行工具。" if event == "tool_use" else "Being 已收到工具结果，正在继续处理。"
        if event == "content_block_delta":
            text = data.get("delta", {}).get("text", "")
            if current.get("callback") and text == "":
                self.save(current)
                return
            if isinstance(text, str):
                current["text"] += text
        # Heart may ack persistence on meta before message_stop. Archive once, keep
        # the ack on the following message_stop so the desktop bubble is not dropped.
        if event == "message_stop" or (isinstance(persisted, dict) and persisted.get("kind") == "self"):
            saved = self.archive_reply(partial=bool(current.get("failed")), source=(persisted or {}).get("from", "final"),
                                       session_id=data.get("session_id"), current=current)
            if saved:
                current["last_persisted"] = saved
                data["persisted"] = saved
                if not current.get("failed"):
                    feedback = {"text": current["text"], "final": True, "id": saved["seq"]}
                current["reply"] += 1
                current["text"] = ""
            elif event == "message_stop" and current.get("last_persisted"):
                data["persisted"] = current["last_persisted"]
        if counted:
            self.emit(event, data, current=current, events=events)
        else:
            current["meta"] = {**data, **({"origin": "callback"} if current.get("callback") else {})}
            self.save(current)
            self.changed.set()
        return feedback

    async def read_response(self, response, current=None, events=None):
        current = self.current if current is None else current
        self.client.check(response)
        if response.status_code == 202:
            raise RuntimeError("Being 已接收请求，但未返回可关联的流；不会自动重发。")
        if "text/event-stream" not in response.headers.get("content-type", ""):
            raise RuntimeError("Being 未返回有效的消息流。")
        event, data, size = "message", [], 0
        async for line in response.aiter_lines():
            if not line:
                if data:
                    feedback = self.consume(event, json.loads("\n".join(data)), current=current, events=events)
                    await self.sync_context(current, feedback)
                event, data, size = "message", [], 0
            elif line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                size += len(line)
                if size > 2_000_000:
                    raise ValueError("Being 消息事件过大。")
                data.append(line[5:].lstrip(" "))
        if data:
            feedback = self.consume(event, json.loads("\n".join(data)), current=current, events=events)
            await self.sync_context(current, feedback)

    async def run(self, body=None, *, recover=False, response=None, current=None, events=None, snapshot=None):
        current = self.current if current is None else current
        try:
            if recover:
                await self.recover(current, events, snapshot=snapshot)
            elif response is not None:
                await self.read_response(response, current, events)
            else:
                current["submitted"] = True
                current.pop("body", None)
                self.save(current)
                async with self.client.http.stream("POST", "api/chat/stream", json=body) as upstream:
                    await self.read_response(upstream, current, events)
        except asyncio.CancelledError:
            self.archive_reply(partial=True, current=current)
            self.save(current)
            raise
        except (httpx.TransportError, asyncio.TimeoutError):
            try:
                await self.recover(current, events)
            except Exception:
                current["failed"] = True
                if not current.get("discard_output"):
                    self.emit("error", {"error": "连接中断，无法确认原请求结果；本地记录已保留，不会自动重发。"}, current=current, events=events)
        except Exception:
            # Do not propagate exceptions containing a credential-bearing URL.
            current["failed"] = True
            if not current.get("discard_output"):
                self.emit("error", {"error": "请求未能完整结束，已保存本地记录；不会自动重复提交。"}, current=current, events=events)
        finally:
            if response is not None:
                await response.aclose()
            if not asyncio.current_task().cancelling():
                persisted = self.archive_reply(partial=bool(current.get("failed")), current=current)
                if persisted:
                    self.emit("message_stop", {"persisted": persisted}, current=current, events=events)
                current["finished"] = True
                current.pop("body", None)
                if current.get("discard_output"):
                    self.emit("stream_discarded", {}, current=current, events=events)
                self.save(current)
                self.changed.set()
                feedback = ({"text": "Being 的回复未能完整结束，请查看聊天记录。", "final": True} if current.get("failed") else
                            {"text": current["text"], "final": True, "id": persisted["seq"]} if persisted else
                            {"text": "", "final": True})
                await self.sync_context(current, feedback)

    def callback_discarded(self, current):
        return bool(self.tasks and any(self.tasks.output_discarded(task_id)
                                      for task_id in current.get("callback_task_ids", ())))

    def callback_replies(self, items, finished):
        # A callback can report its child ID after the first text delta.
        start = 0
        for end, item in enumerate(items, 1):
            if item["event"] == "message_stop":
                yield items[start:end]
                start = end
        if finished and start < len(items):
            yield items[start:]

    def prepare_callback(self, current, active, items):
        source_id = current.setdefault("callback_source_id", active.get("task_id"))
        reply_source = None
        scene_id = current.get("stream_scene")
        text_parts = []
        for index, item in enumerate(items):
            event, data = item["event"], item["data"]
            if not isinstance(data, dict):
                continue
            if event == "meta":
                if data.get("continuation") or "task_id" in data:
                    source_id = data.get("task_id")
                    reply_source = source_id
                if "scene_id" in data:
                    scene_id = data["scene_id"]
            if event == "content_block_delta" and isinstance(data.get("delta"), dict):
                text = data["delta"].get("text")
                if isinstance(text, str):
                    text_parts.append((index, text))
        current["callback_source_id"] = source_id
        marker = CALLBACK_MARKER.match("".join(text for _, text in text_parts))
        attribution = reply_source or (marker[1] if marker else source_id)
        owner = self.tasks.callback_owner(attribution, scene_id)
        current.pop("callback_scene", None)
        if marker:
            remaining = marker.end()
            for index, text in text_parts:
                if not remaining:
                    break
                item = items[index]
                data = item["data"]
                items[index] = {**item, "data": {**data, "delta": {**data["delta"], "text": text[remaining:]}}}
                remaining = max(0, remaining - len(text))
        if isinstance(attribution, str) and attribution:
            current["callback_task_ids"] = [owner] if owner else []
        else:
            receipt = {
                "subagents": dict(current.get("subagents", {})),
                "subagentCalls": dict(current.get("subagentCalls", {})),
            }
            owners = set()
            for item in items:
                if item["event"] in {"tool_use", "tool_result"} and isinstance(item["data"], dict):
                    for child in observe(receipt, item["event"], item["data"]) or ():
                        if parent := self.tasks.callback_owner(child, scene_id):
                            owners.add(parent)
            owner = next(iter(owners)) if len(owners) == 1 else None
            current["callback_task_ids"] = [owner] if owner else []
        if scene_id is None and owner in self.tasks.tasks:
            current["callback_scene"] = self.tasks.tasks[owner]["scene"]

    async def recover(self, current=None, events=None, *, snapshot=None):
        current = self.current if current is None else current
        upstream = current.get("upstream_id")
        if not upstream and not current.get("accepted"):
            raise RuntimeError("No correlated stream; never resubmit")
        sequence = current.get("upstream_seq", 0)
        while True:
            if snapshot is not None:
                active, snapshot = snapshot, None
                requested_after = 0
            else:
                response = await self.client.http.get("api/stream/active", params={"after": sequence}, timeout=15)
                self.client.check(response)
                active = None if response.status_code == 204 else response.json()
                requested_after = sequence
            if current.get("accepted") and not active:
                await asyncio.sleep(0.5)
                continue
            if current.get("accepted") and (not upstream or (active and active.get("stream_id") != upstream)):
                # A 202 joins Heart's active stream, just as Desktop's catch-up watcher does.
                upstream = current["upstream_id"] = active["stream_id"]
                current["stream_scene"] = active.get("scene_id")
                sequence = current["upstream_seq"] = 0
                self.save(current)
                if requested_after != 0:
                    continue
            if not active or active.get("stream_id") != upstream:
                raise RuntimeError("Original stream is unavailable")
            items = [item for item in active.get("events", []) if item["seq"] > sequence]
            correlate = current.get("callback") and self.tasks is not None
            replies = self.callback_replies(items, active.get("finished")) if correlate else (items,)
            for reply in replies:
                if correlate:
                    self.prepare_callback(current, active, reply)
                for item in reply:
                    if item["seq"] != sequence + 1:
                        raise RuntimeError("Original stream replay has a gap")
                    feedback = self.consume(item["event"], item["data"], item["seq"], current=current, events=events)
                    sequence = item["seq"]
                    await self.sync_context(current, feedback)
            if active.get("finished") and (not current.get("accepted") or current["reply"]):
                return
            await asyncio.sleep(0.5)

    async def submit(self, request):
        body = await request.json()
        async with self.admission:
            prepared = await self.prepare(body)
        if isinstance(prepared, Response):
            return prepared
        return await self.submit_body(*prepared)

    async def prepare(self, body):
        if (not isinstance(body, dict) or not isinstance(body.get("message"), str)
                or not isinstance(body.get("scene_id"), str) or not 1 <= len(body["scene_id"]) <= 128
                or not isinstance(body.get("scene_meta", {}), dict)):
            return JSONResponse({"error": "Invalid chat message or scene"}, 400)
        request_id = body.pop("client_request_id", None) or uuid4().hex
        if not isinstance(request_id, str) or len(request_id) > 128:
            return JSONResponse({"error": "Invalid request ID"}, 400)
        receipt = self.history.state("request:" + request_id)
        if receipt and (receipt.get("uncertain") or receipt.get("status") == "sending"):
            return JSONResponse({"error": "原请求结果尚未确认，不会自动重复提交。"}, 502)
        if receipt and not (isinstance(receipt.get("status"), int) and receipt["status"] >= 400):
            return Response(status_code=202)
        if self.current and self.current.get("discard_output") and not self.current["finished"]:
            return JSONResponse({"error": "上一条文字请求仍在结束，请稍后发送。"}, 409)
        if self.before_send:
            await self.before_send(body["scene_id"], request_id)
        scene = {"scene_id": body["scene_id"], "scene_meta": body.get("scene_meta", {})}
        session_id = body.get("session_id") if isinstance(body.get("session_id"), str) and body["session_id"] else None
        current = self.current
        splicing = bool(current and not current.get("finished"))
        self.history.put("request:" + request_id, {"role": "user", "content": body["message"],
                         "scene_id": scene["scene_id"], "scene_label": scene["scene_meta"].get("scene_label", ""),
                          "source": "text-user", **({"beingSessionId": session_id} if session_id else {})})
        # User text goes to Being as-is; no local router and no conversation_context appendix.
        context_receipt = []
        self.request_order += 1
        if splicing:
            current.setdefault("scenes", {current["scene"]["scene_id"]: current["scene"]})[scene["scene_id"]] = scene
            self.save()
        else:
            current = self.current = self.new_stream(scene, request_id, self.request_order)
            current["opening"] = request_id
            self.events = []
            self.history.save_chat(current, reset=True)
        self.history.state("request:" + request_id, {"status": "sending"})
        prepared = body, scene, request_id, current, self.events, self.request_order, context_receipt
        if self.context_changed:
            await self.context_changed(scene["scene_id"])
        return prepared

    async def submit_body(self, body, scene, request_id, current, events, order, context_receipt):
        response = None
        try:
            response = await self.client.http.send(self.client.http.build_request("POST", "api/chat/stream", json=body), stream=True)
        except httpx.TransportError:
            self.history.state("request:" + request_id, {"uncertain": True})
            if current.get("opening") == request_id:
                current.pop("opening")
                current["finished"] = not current.get("accepted", False)
                self.save(current)
                if current.get("accepted"):
                    self.start_run(current=current, events=events, recover=True)
                self.changed.set()
            await self.sync_context(current, {"text": "文字请求的发送结果尚未确认，请查看聊天记录。", "final": True}, scene=scene)
            return JSONResponse({"error": "发送结果无法确认，请核对记录；不会自动重复提交。"}, 502)
        headers = {key: response.headers[key] for key in ("content-type", "retry-after") if key in response.headers}
        if self.client.context and (response.status_code == 202 or
                                   (response.status_code == 200 and "text/event-stream" in headers.get("content-type", ""))):
            self.client.context.acknowledge(context_receipt)
        if response.status_code != 200 or "text/event-stream" not in response.headers.get("content-type", ""):
            content = await response.aread()
            await response.aclose()
            if response.status_code != 202:
                self.history.state("request:" + request_id, {"status": response.status_code})
                if current.get("opening") == request_id:
                    current.pop("opening")
                    current["finished"] = not current.get("accepted", False)
                    current["failed"] = current["finished"]
                    self.save(current)
                    if current.get("accepted"):
                        self.start_run(current=current, events=events, recover=True)
                    self.changed.set()
                await self.sync_context(current, {"text": "Being 未能返回有效回复，请查看聊天中的详情。", "final": True}, scene=scene)
                return Response(content, status_code=response.status_code, headers=headers)
            if current is not self.current:
                current, events = self.current, self.events
                current.setdefault("scenes", {current["scene"]["scene_id"]: current["scene"]})[scene["scene_id"]] = scene
            if current.get("opening") == request_id:
                current.pop("opening")
            current.update(finished=False, accepted=True)
            self.history.state("request:" + request_id, {"stream_id": current["stream_id"]})
            self.save(current)
            reader = self.readers.get(current["stream_id"])
            if not current.get("opening") and (reader is None or reader.done()):
                self.start_run(current=current, events=events, recover=True)
            await self.sync_context(current, scene=scene)
            return Response(content, status_code=202, headers=headers)
        if current.get("opening") == request_id:
            current.pop("opening")
            self.save(current)
        else:
            self.archive_reply(partial=True, current=current)
            current, events = self.new_stream(scene, request_id, order), []
            if order >= self.current.get("order", 0):
                self.current, self.events = current, events
                self.history.save_chat(current, reset=True)
        self.history.state("request:" + request_id, {"stream_id": current["stream_id"]})
        self.start_run(current=current, events=events, response=response)
        await self.sync_context(current, scene=scene)
        return self.stream_response(current, events)

    def visible_event(self, current, item):
        if item["event"] in {"stream_stopped", "stream_discarded"}:
            return True
        if current.get("routed") and "task_id" in item and item["task_id"] is None:
            return True
        task_id = item.get("task_id") or current.get("task_id")
        return not (current.get("discard_output")
                    or task_id and self.tasks and self.tasks.output_discarded(task_id))

    def discard_finished_task(self, task_id):
        current = self.current
        if (current and current.get("routed") and current.get("task_id") == task_id
                and current["finished"] and not current.get("discard_output")):
            current.update(discard_output=True, text="")
            self.emit("stream_discarded", {})

    def stream_response(self, current, events):
        async def stream():
            after = 0
            meta = None
            while True:
                self.changed.clear()
                if current.get("meta") != meta:
                    meta = current["meta"]
                    yield f"event: meta\ndata: {json.dumps({**meta, 'stream_id': current['stream_id']}, ensure_ascii=False)}\n\n"
                for item in events:
                    if item["seq"] > after:
                        after = item["seq"]
                        if self.visible_event(current, item):
                            yield f"event: {item['event']}\ndata: {json.dumps(item['data'], ensure_ascii=False)}\n\n"
                if current["finished"]:
                    return
                try:
                    await asyncio.wait_for(self.changed.wait(), 15)
                except TimeoutError:
                    yield ": keepalive\n\n"
        return StreamingResponse(stream(), media_type="text/event-stream")

    async def stop(self, request):
        async with self.admission:
            return await self.stop_body(await request.json())

    def voice_stop_target(self, stream_id, scene_id, order):
        current = self.current
        if (current and current["stream_id"] == stream_id and order == self.request_order and not current.get("callback")
                and current.get("reply_scene", current["scene"])["scene_id"] == scene_id
                and all(scene == scene_id for scene in current.get("scenes", {}))
                and (not current["finished"] or pending(current))):
            return current

    def start_voice_stop(self, stream_id, scene_id, order):
        current = self.voice_stop_target(stream_id, scene_id, order)
        if not current:
            return False
        self.discard_voice_output(current)
        if stream_id not in self.voice_stops:
            job = asyncio.create_task(self.stop_voice_when_ready(stream_id, scene_id, order))
            self.voice_stops[stream_id] = job

            def finished(done):
                self.voice_stops.pop(stream_id, None)
                if not done.cancelled():
                    done.exception()

            job.add_done_callback(finished)
        return True

    def discard_voice_output(self, current):
        if not current.get("discard_output"):
            current.update(discard_output=True, text="", progress="")
            self.save(current)

    async def stop_voice_when_ready(self, stream_id, scene_id, order):
        try:
            async with asyncio.timeout(15):
                while (current := self.voice_stop_target(stream_id, scene_id, order)) and not current.get("upstream_id"):
                    await asyncio.sleep(0.1)
        except TimeoutError:
            return
        await self.stop_voice(stream_id, scene_id, order)

    async def stop_voice(self, stream_id, scene_id, order):
        async with self.admission:
            current = self.voice_stop_target(stream_id, scene_id, order)
            if not current:
                return {"text_work": "ended", "user_message": "刚才那条文字请求已结束或已切换。"}
            self.discard_voice_output(current)
            try:
                await self.stop_body({"stream_id": stream_id}, voice=True)
            except httpx.TransportError:
                pass  # Do not expose the credential-bearing request URL.
            if current.get("stopped"):
                return {"text_work": "stopped", "user_message": "文字请求已停止。"}
            return {"status": "unknown", "error": True, "user_message": "文字请求尚未确认停止。"}

    async def finish_stop(self, current, events, *, feedback=None):
        job = self.readers.get(current["stream_id"])
        if job:
            job.cancel()
            await asyncio.gather(job, return_exceptions=True)
        current["finished"] = current["stopped"] = True
        self.emit("stream_stopped", {}, current=current, events=events)
        await self.sync_context(current, feedback)

    async def stop_body(self, body, *, voice=False):
        if not isinstance(body, dict) or not self.current or body.get("stream_id") != self.current["stream_id"]:
            return JSONResponse({"error": "Unknown local stream"}, 409)
        current, events = self.current, self.events
        if current.get("routed"):
            task = self.routed_task(current)
            if task and task["status"] not in TERMINAL:
                try:
                    await self.tasks.cancel(task["id"], discard_output=True)
                except (ValueError, RuntimeError, httpx.TransportError):
                    return JSONResponse({"error": "Being 尚未确认停止任务。"}, 409)
            if not current["finished"] or (task and task["status"] == "cancelled" and not current.get("stopped")):
                await self.finish_stop(current, events)
            return {"ok": True, "stream_id": body["stream_id"]}
        feedback = {"text": "" if voice else "Being 已停止处理这条文字请求。", "final": True}
        if pending(current) and not current.get("callback"):
            receipt = {key: current[key] for key in ("subagents", "subagentCalls") if key in current}
            receipt.update(submitted=True, stream_id=current.get("upstream_id"), stream_finished=current["finished"])
            try:
                await self.client.cancel({"being": receipt, "scene": current["scene"]})
            except (ValueError, RuntimeError):
                current["subagents"] = receipt.get("subagents", {})
                self.save(current)
                return JSONResponse({"error": "子代理停止尚未确认，请在 Being 聊天中核对；不会报告全部已取消。"}, 409)
            current["subagents"] = receipt.get("subagents", {})
            await self.finish_stop(current, events, feedback={"text": "", "final": True})
            return {"ok": True, "stream_id": body["stream_id"]}
        if not current["finished"]:
            upstream = current.get("upstream_id")
            if not upstream:
                return JSONResponse({"error": "Being 尚未返回流标识，请稍后停止。"}, 409)
            response = await self.client.http.post("api/stop", json={**body, "stream_id": upstream}, timeout=15)
            content = response.content
            receipt = None
            if response.status_code != 204:
                try:
                    receipt = response.json()
                except ValueError:
                    pass
            confirmed = response.status_code == 204 or (response.is_success and response.status_code != 202
                and isinstance(receipt, dict) and (receipt.get("ok") is True or receipt.get("stopped") is True)
                and receipt.get("ok") is not False and receipt.get("stopped") is not False
                and receipt.get("stream_id", upstream) == upstream)
            if isinstance(receipt, dict) and receipt.get("stream_id") == upstream:
                receipt["stream_id"] = body["stream_id"]
                content = json.dumps(receipt).encode()
            if not confirmed:
                return Response(content, status_code=response.status_code,
                                headers={"content-type": response.headers.get("content-type", "application/json")})
            await self.finish_stop(current, events, feedback=feedback)
            return Response(content, status_code=response.status_code,
                            headers={"content-type": response.headers.get("content-type", "application/json")})
        return {"ok": True, "stream_id": body["stream_id"]}

    async def close(self):
        if self.callback_observer:
            self.callback_observer.cancel()
            await asyncio.gather(self.callback_observer, return_exceptions=True)
        await asyncio.gather(*self.voice_stops.values(), return_exceptions=True)
        readers = list(self.readers.values())
        for job in readers:
            job.cancel()
        await asyncio.gather(*readers, return_exceptions=True)


def register_chat(app):
    @app.get("/api/heart/history")
    @app.get("/api/local-history")
    async def history(request: Request, limit: int = 100, after: int | None = None,
                      before: int | None = None, scene: str | None = None):
        archive = request.state.gateway.history
        return {"messages": archive.page(after=after, before=before, scene=scene, limit=limit),
                "lastSeq": archive.last_seq()}

    @app.get("/api/local-history/scenes")
    async def scenes(request: Request):
        return {"scenes": request.state.gateway.history.scenes()}

    @app.post("/api/local-history/import")
    async def import_history(request: Request):
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError("Invalid legacy history import")
        complete = request.state.gateway.history.import_legacy(
            body.get("messages"), complete=body.get("complete") is True)
        return {"ok": True, "complete": complete}

    @app.post("/api/heart/chat/stream")
    async def send(request: Request):
        gateway = request.state.gateway
        if (name := request.headers.get("x-portal-being-name")) is not None:
            if len(name) > 4800:
                raise ValueError("Invalid being_name")
            gateway.set_desktop_display_name(unquote(name, errors="strict"))
        return await gateway.chat.submit(request)

    @app.get("/api/heart/stream/active")
    async def active(request: Request, after: int = 0):
        chat = request.state.gateway.chat
        if not chat.current:
            return Response(status_code=204)
        return {**chat.current.get("meta", {}), "stream_id": chat.current["stream_id"], "finished": chat.current["finished"],
                **({"awaiting_subagents": True} if pending(chat.current) else {}),
                **({"stopped": True} if chat.current.get("stopped") else {}),
                **({"discarded": True} if chat.current.get("discard_output") else {}),
                "next_seq": chat.current["sequence"] + 1,
                **chat.current["scene"], "events": [item for item in chat.events
                                                   if item["seq"] > after and chat.visible_event(chat.current, item)]}

    @app.post("/api/heart/stop")
    async def stop(request: Request):
        return await request.state.gateway.chat.stop(request)

    @app.post("/api/scene")
    async def scene(request: Request):
        body = await request.json()
        if (not isinstance(body, dict) or not isinstance(body.get("scene_id"), str) or not 1 <= len(body["scene_id"]) <= 128
                or not isinstance(body.get("scene_meta", {}), dict)):
            return JSONResponse({"error": "Invalid scene"}, 400)
        gateway = request.state.gateway
        if gateway.active and not gateway.active.closed:
            await gateway.active.close()
        gateway.settings.being_scene = body
        gateway.store.scene = body
        return {"ok": True}
