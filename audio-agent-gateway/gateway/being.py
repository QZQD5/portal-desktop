"""Heart HTTP/SSE task transport. Disconnecting never cancels a remote task."""

import asyncio
import json
import logging
import re

import httpx

from .being_context import BeingContext
from .config import validate_being_endpoint
from .content import embedded
from .logging import redact
from .subagent_tasks import TERMINAL, observe, pending, require_stopped

log = logging.getLogger("gateway.being")
TOOL_PROGRESS_MESSAGES = {
    "tool_use": "Being 正在执行工具。",
    "tool_result": "Being 已收到工具结果。",
}
MEMORY_WRITE_TOOL = "update_being_memory"
MEMORY_WRITE_KIND = "being_memory_write"


class BeingClient:
    def __init__(self, settings, *, transport=None, store=None, tasks=None):
        endpoint = validate_being_endpoint(settings.being_endpoint)
        if not settings.being_token or not settings.being_scene.get("scene_id"):
            raise ValueError("Desktop must provide a Being token and scene")
        self.settings = settings
        self.context = BeingContext(store, endpoint, tasks=tasks) if store else None
        self.http = httpx.AsyncClient(base_url=endpoint + "/", params={"token": settings.being_token},
                                      timeout=httpx.Timeout(30, read=None), follow_redirects=False, transport=transport)
        self.ready, self.error, self.info = False, None, {}
        self.turn_lock = asyncio.Lock()

    def status(self):
        return {"status": "ready" if self.ready else "unavailable"}

    @property
    def name(self):
        if isinstance(self.info, dict):
            for key in ("being_name", "name"):
                value = self.info.get(key)
                if isinstance(value, str) and value.strip():
                    return " ".join(value.split())[:128]
        return None

    async def start(self):
        response = await self.http.get("api/status", timeout=15)
        self.check(response)
        self.info = response.json()
        self.ready, self.error = True, None

    @staticmethod
    def check(response):
        # httpx's default exception includes the credential-bearing request URL.
        if not response.is_success:
            raise RuntimeError(f"Being API returned HTTP {response.status_code}")

    async def active(self):
        response = await self.http.get("api/stream/active", timeout=15)
        self.check(response)
        return None if response.status_code == 204 else response.json()

    async def update(self, task, objective, user_text=""):
        """Splice a correction without waiting for the original run's turn lock."""
        receipt = task.get("being", {})
        scene = task.get("scene", self.settings.being_scene)
        if (receipt.get("endpoint") != self.settings.being_endpoint
                or receipt.get("scene_id") != scene["scene_id"] or not receipt.get("stream_id")):
            raise ValueError("尚未取得可关联的原任务流，暂不能发送修正。")
        active = await self.active()
        if task.get("discard_output"):
            raise ValueError("原任务的取消已受理。")
        if (not active or active.get("stream_id") != receipt["stream_id"]
                or active.get("finished") is True):
            raise ValueError("原任务流已结束或发生变化，请查询原任务后再继续。")
        if active.get("scene_id", scene["scene_id"]) != scene["scene_id"]:
            raise ValueError("原任务场景已变化，不能发送修正。")
        message = (
            "用户修正当前工作的要求，请用以下内容更新与之冲突的部分，并保留原任务中仍适用的要求和已完成的步骤。\n"
            + objective
        )
        if user_text and user_text != objective:
            message += "\n用户本轮原话（与上述转述冲突时以用户原话为准）：\n" + user_text
        # The same chat endpoint is used by Desktop's busy-scene splice path.
        # Only 202 confirms splice acceptance; it never proves application/completion.
        async with self.http.stream("POST", "api/chat/stream", json={"message": message, **scene}) as response:
            self.check(response)
            if response.status_code != 202:
                raise RuntimeError("修正请求未取得插话确认，发送结果待核对；不会自动重发。")

    async def run(self, task, context, save, progress, output=None):
        async with self.turn_lock:
            return await self._run(task, context, save, progress, output)

    async def _run(self, task, context, save, progress, output=None):
        scene = task.get("scene", self.settings.being_scene)
        receipt = task.setdefault("being", {"endpoint": self.settings.being_endpoint,
                                             "scene_id": scene["scene_id"]})
        if receipt["endpoint"] != self.settings.being_endpoint or receipt["scene_id"] != scene["scene_id"]:
            raise RuntimeError("Being task belongs to a different connection or scene")
        if receipt.get("submitted"):
            return await self.recover(task, save, progress, output)
        # Do not splice a voice request into an unrelated running text turn.
        while True:
            active = await self.active()
            if not active or active.get("finished") is True:
                break
            if task["status"] == "cancelled":
                return ""
            await progress("Being 正在处理其他请求，任务排队中。")
            await asyncio.sleep(1)
        context_receipt = []
        if self.context:
            frozen = self.context.store.get("task_context:" + task["id"], {})
            if frozen:
                context, context_receipt = self.context.prepare(scene["scene_id"], frozen,
                                                                session_id=scene.get("session_id"))
        message = task["objective"]
        if prior := task.get("priorObjectives"):
            message += ("\n\n以上是用户最新的修正原话。下方是原请求和之前的修正，按时间排列。"
                        "保留仍适用的要求，冲突时以最新修正为准。\n" + "\n".join(prior))
        elif (user_text := task.get("lastUpdate", {}).get("userText")) and user_text != message:
            message += "\n用户本轮纠正原话（优先于旧理解）：\n" + user_text
        if task.get("kind") == "scheduled_task":
            message += "\n\nGateway 已到设定的触发时间，请现在执行用户要求的任务。"
        if context:
            message += "\n\n以下是语音会话背景，仅用于理解本次请求，不要再次执行历史任务：\n" + context
            if task_id := task.get("id"):
                message += ("\n\n如果委派子代理，请在委派说明中保留以下原任务标记。"
                            "收到该子代理的回调后，每条转达本任务结果的回复首行都原样保留标记，下一行开始写正文。"
                            "各任务的结果分别回复。当前直接回复无需该标记。\n"
                            f"<!-- gateway-task: {task_id} -->")
        body = {"message": message, **scene}
        attachments = []
        for part in task.get("parts", []):
            if part["type"] == "text":
                body["message"] += "\n" + part["text"]
            elif data := embedded(part.get("url", "")):
                attachments.append({"media_type": data[0], "data": data[1]})
            else:
                raise ValueError("Voice task attachments must contain inline data")
        if task.get("kind") == MEMORY_WRITE_KIND:
            body["message"] += ("\n\n请由当前 Being 直接处理本次身份或长期记忆写入，"
                                "用户为你指定新名字时更新正式显示名。"
                                "按实际结果简短回复，避免委派子代理或另行通知。")
        if attachments:
            body["attachments"] = attachments
        receipt["submitted"] = True
        save()
        pieces = []
        try:
            async with self.http.stream("POST", "api/chat/stream", json=body) as response:
                self.check(response)
                if response.status_code == 202 or "text/event-stream" in response.headers.get("content-type", ""):
                    receipt["accepted"] = True
                    save()
                    if self.context:
                        self.context.acknowledge(context_receipt)
                if response.status_code == 202:
                    # shortcut: Heart has no documented request lookup for 202; never guess ownership or resend.
                    raise RuntimeError("Being 已接收请求，但未返回可关联的任务流；请在聊天中查看结果。不会自动重发。")
                if "text/event-stream" not in response.headers.get("content-type", ""):
                    raise RuntimeError("Being did not return an SSE stream")
                await self.read_response(response, task, pieces, save, progress, output)
        except (httpx.TransportError, asyncio.TimeoutError):
            return await self.recover(task, save, progress, output)
        receipt["stream_finished"] = True
        save()
        return "".join(pieces).strip()

    async def read_response(self, response, task, pieces, save, progress, output=None):
        event, data, size = "message", [], 0
        async for line in response.aiter_lines():
            if not line:
                if data:
                    await self.consume(task, event, json.loads("\n".join(data)), pieces, save, progress, output)
                event, data, size = "message", [], 0
            elif line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                size += len(line)
                if size > 2_000_000:
                    raise ValueError("Being SSE event too large")
                data.append(line[5:].lstrip(" "))
        if data:
            await self.consume(task, event, json.loads("\n".join(data)), pieces, save, progress, output)

    async def consume(self, task, event, data, pieces, save, progress, output=None):
        if not isinstance(data, dict):
            return
        receipt = task["being"]
        if event == "meta" and data.get("stream_id"):
            if receipt.get("stream_id") and receipt["stream_id"] != data["stream_id"]:
                raise RuntimeError("Being stream identity changed")
            receipt["stream_id"] = data["stream_id"]
            save()
        if "scene_id" in data:
            receipt["stream_scene"] = data["scene_id"]
            save()
        if receipt.get("stream_scene", receipt.get("scene_id")) != receipt.get("scene_id"):
            return
        if event in {"tool_use", "tool_result"}:
            observe(receipt, event, data)
            save()
        if event in {"thinking", "reasoning", "tool_use", "tool_result", "message_stop", "error"}:
            observer = getattr(progress, "activity", None)
            if observer:
                await observer(event, data)
        if event == "content_block_delta":
            if task.get("discard_output"):
                pieces.clear()
                return
            text = data.get("delta", {}).get("text", "")
            if isinstance(text, str):
                pieces.append(text)
                if text and output:
                    await output("".join(pieces))
        elif event == "message_stop":
            if data.get("session_id"):
                task["being"]["session_id"] = data["session_id"]
                save()
            pieces.append("\n\n")
        elif event in {"tool_use", "tool_result"}:
            await progress(TOOL_PROGRESS_MESSAGES[event])
        elif event == "error":
            message = self.error_message(data)
            log.warning("being.stream_error task=%s stream=%s message=%s",
                        task.get("id", "unknown"), receipt.get("stream_id", "unknown"), message)
            raise RuntimeError(message)

    def error_message(self, data):
        # Keep the upstream reason available in task/history instead of pointing
        # users to details that the voice transport never saved.
        error = data.get("error")
        nested = error if isinstance(error, dict) else {}
        detail = next((value.strip() for value in (
            data.get("message"), data.get("user_message"), nested.get("message"), error,
        ) if isinstance(value, str) and value.strip()), "")
        code = data.get("code") or nested.get("code")
        if isinstance(code, (str, int)) and not isinstance(code, bool):
            detail = f"{code}：{detail}" if detail else str(code)
        if not detail:
            return "任务执行失败：Being 未提供具体错误原因，本次未取得完整结果。"
        stalled = re.search(r"LLM stream stalled: no data received for (\d+)s", detail, re.I)
        if stalled:
            return f"模型回复流连续 {stalled[1]} 秒没有新数据，Being 已中断本轮回复。"
        for secret in (self.settings.being_token, self.settings.doubao_api_key):
            if secret:
                detail = detail.replace(secret, "[REDACTED]")
        detail = redact(detail)
        detail = re.sub(r"https?://[^\s<>]+", "[链接已省略]", detail)
        detail = " ".join(detail.split())[:600]
        return "任务执行失败：" + detail

    async def recover(self, task, save, progress, output=None):
        stream_id = task.get("being", {}).get("stream_id")
        if not stream_id:
            raise RuntimeError("请求可能已经提交 Being，但未取得流标识；请在聊天中核对。不会自动重发。")
        task["being"].pop("stream_scene", None)
        pieces, sequence, reset_activity = [], 0, False
        while True:
            response = await self.http.get("api/stream/active", params={"after": sequence}, timeout=15)
            self.check(response)
            active = None if response.status_code == 204 else response.json()
            if not active or active.get("stream_id") != stream_id:
                raise RuntimeError("原 Being 任务流已不可回放；请在聊天历史中核对结果。不会自动重发。")
            if not reset_activity:
                observer = getattr(progress, "activity", None)
                if observer:
                    await observer("reset", {})
                reset_activity = True
            for item in active.get("events", []):
                if item["seq"] <= sequence:
                    continue
                await self.consume(task, item["event"], item["data"], pieces, save, progress, output)
                sequence = item["seq"]
            if active.get("finished") is True:
                task["being"]["stream_finished"] = True
                save()
                return "".join(pieces).strip()
            await asyncio.sleep(0.5)

    async def cancel(self, task):
        receipt = task.get("being", {})
        if not receipt.get("stream_finished"):
            await self.stop_stream(task)
            receipt["stream_finished"] = True
        if not pending(receipt):
            return
        if any(operation == "spawn" for operation in receipt.get("subagentCalls", {}).values()):
            require_stopped(receipt)
        children = {key: status for key, status in receipt.get("subagents", {}).items() if status not in TERMINAL}
        scene = task.get("scene", self.settings.being_scene)
        control = {"being": {"scene_id": scene["scene_id"], "subagents": children.copy()}}
        message = ("用户要求取消原工作。只取消以下已有子任务，不创建新工作，不操作其他任务。"
                   "逐项调用 portal_subagent_control(action=cancel, task_id=对应ID)；若已结束，"
                   "调用 portal_subagent_status 核实。必须返回实际工具回执，不要仅口头确认：\n"
                   + json.dumps(list(children)))

        async def progress(_):
            pass

        def save_control():
            receipt.setdefault("subagents", {}).update(control["being"].get("subagents", {}))

        try:
            async with self.http.stream("POST", "api/chat/stream", json={"message": message, **scene},
                                        timeout=httpx.Timeout(30, read=30)) as response:
                self.check(response)
                if response.status_code != 200 or "text/event-stream" not in response.headers.get("content-type", ""):
                    raise RuntimeError("子代理取消请求未取得可核对的工具回执，请在聊天中核对；不会自动重发。")
                await self.read_response(response, control, [], save_control, progress)
        except httpx.TransportError:
            raise RuntimeError("子代理取消请求的结果尚未确认，请在聊天中核对；不会自动重发。") from None
        save_control()
        require_stopped(receipt)

    async def stop_stream(self, task):
        receipt = task.get("being", {})
        if not receipt.get("submitted"):
            return
        if not receipt.get("stream_id"):
            raise RuntimeError("尚未取得 Being 流标识，不能安全取消；请在聊天中停止对应任务。")
        response = await self.http.post("api/stop", json={"stream_id": receipt["stream_id"]}, timeout=15)
        self.check(response)
        if response.status_code == 202:
            raise RuntimeError("Being 正在处理取消请求，尚未确认停止。")
        if response.status_code != 204:
            try:
                result = response.json()
            except ValueError:
                # A successful non-JSON receipt still needs independent stream confirmation.
                status = await self.http.get("api/stream/active", timeout=15)
                self.check(status)
                if status.status_code == 204:
                    return
                try:
                    active = status.json()
                except ValueError:
                    active = None
                if (isinstance(active, dict) and active.get("stream_id") == receipt["stream_id"]
                        and active.get("finished") is True):
                    return
                raise RuntimeError("Being 未确认停止指定任务，任务仍需跟踪。") from None
            if (not isinstance(result, dict) or (result.get("ok") is not True and result.get("stopped") is not True)
                    or result.get("ok") is False or result.get("stopped") is False
                    or result.get("stream_id", receipt["stream_id"]) != receipt["stream_id"]):
                raise RuntimeError("Being 未确认停止指定任务，任务仍需跟踪。")

    async def close(self):
        self.ready = False
        await self.http.aclose()
