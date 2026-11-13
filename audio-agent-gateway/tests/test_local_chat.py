import asyncio
import json

import httpx
import pytest
from starlette.requests import Request

from gateway.being import BeingClient
from gateway.chat import Chat
from gateway.config import Settings
from gateway.conversation_context import snapshot
from gateway.local_history import LocalHistory
from gateway.store import Store


def request(body):
    async def receive():
        return {"type": "http.request", "body": json.dumps(body).encode(), "more_body": False}
    return Request({"type": "http", "method": "POST", "path": "/", "headers": []}, receive)


def event(kind, body):
    return f"event: {kind}\ndata: {json.dumps(body, ensure_ascii=False)}\n\n".encode()


class Stream(httpx.AsyncByteStream):
    def __init__(self, chunks, gate=None):
        self.chunks, self.gate = chunks, gate

    async def __aiter__(self):
        for chunk in self.chunks:
            for index in range(0, len(chunk), 3):
                yield chunk[index:index + 3]
        if self.gate:
            await self.gate.wait()


def setup(tmp_path, chunks, gate=None):
    sent = []

    async def respond(req):
        assert req.url.params["token"] == "private"
        if req.url.path.endswith("/stop"):
            sent.append(json.loads(req.content))
            return httpx.Response(200, json={"ok": True})
        if req.method == "GET":
            return httpx.Response(204)
        sent.append(json.loads(req.content))
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream(chunks, gate))

    client = BeingClient(Settings(being_endpoint="http://127.0.0.1/being", being_token="private",
                         being_scene={"scene_id": "room"}), transport=httpx.MockTransport(respond))
    history = LocalHistory(tmp_path / "history.sqlite3")
    return Chat(client, history), sent


async def test_text_without_voice_key_archives_replies_with_local_sequence_and_idempotent_admission(tmp_path):
    chat, sent = setup(tmp_path, [event("meta", {"stream_id": "remote-1"}),
        event("content_block_delta", {"delta": {"text": "开始"}}),
        event("message_stop", {"persisted": {"seq": 99, "kind": "self"}}),
        event("tool_use", {"name": "test"}), event("content_block_delta", {"delta": {"text": "完成"}})])
    body = {"message": "测试", "scene_id": "room", "client_request_id": "request-one"}
    try:
        response = await chat.submit(request(body))
        stream = "".join([chunk async for chunk in response.body_iterator])
        rows = chat.history.page()
        assert [row["content"] for row in rows] == ["测试", "开始", "完成"]
        assert [row["seq"] for row in rows] == [1, 2, 3]
        assert '"seq": 99' not in stream and '"seq": 2' in stream
        assert "client_request_id" not in sent[0]
        assert (await chat.submit(request(body))).status_code == 202
        assert len(sent) == 1
        assert chat.current["finished"]
    finally:
        await chat.close()
        await chat.client.close()
        chat.history.close()


async def test_ui_disconnect_keeps_archiving_and_stop_targets_only_its_upstream_stream(tmp_path):
    gate = asyncio.Event()
    chat, sent = setup(tmp_path, [event("meta", {"stream_id": "remote-2"}),
                                event("content_block_delta", {"delta": {"text": "部分结果"}})], gate)
    try:
        response = await chat.submit(request({"message": "执行", "scene_id": "room"}))
        iterator = response.body_iterator
        await anext(iterator)
        await iterator.aclose()
        async with asyncio.timeout(2):
            while chat.current["text"] != "部分结果":
                await asyncio.sleep(.01)
        assert not chat.job.done()
        assert (await chat.stop(request({"stream_id": "wrong"}))).status_code == 409
        result = await chat.stop(request({"stream_id": chat.current["stream_id"]}))
        assert json.loads(result.body)["ok"]
        assert sent[-1] == {"stream_id": "remote-2"}
        assert chat.history.page()[-1]["content"] == "部分结果"
        assert chat.current["finished"]
    finally:
        gate.set()
        await chat.close()
        await chat.client.close()
        chat.history.close()


def test_voice_history_survives_journal_pruning_restart_and_imports_without_duplicates(tmp_path):
    history = LocalHistory(tmp_path / "history.sqlite3")
    store = Store(tmp_path / "gateway.sqlite3")
    store.attach_history(history, {"scene_id": "voice-room"})
    for index in range(110):
        store.message("voice-session", "user", str(index), id=f"message-{index}")
    assert len(store.history("voice-session", limit=200)) == 100
    assert len(history.page(limit=500)) == 110
    history.import_legacy([{"seq": 1, "role": "user", "content": "旧文字", "scene_id": "text-room"}])
    history.import_legacy([{"seq": 1, "role": "user", "content": "旧文字", "scene_id": "text-room"}])
    assert history.page(scene="text-room")[0]["seq"] != 1
    store.close()
    history.close()
    reopened = LocalHistory(tmp_path / "history.sqlite3")
    try:
        reopened.import_gateway(tmp_path / "gateway.sqlite3", {"scene_id": "wrong-room"})
        assert len(reopened.page(limit=500)) == 111
        assert len(reopened.page(scene="voice-room", limit=500)) == 110
        assert not reopened.page(scene="wrong-room")
    finally:
        reopened.close()


async def test_legacy_import_completion_survives_reopening_history(running):
    gateway, _, http, _ = running
    path = "/api/local-history/import"
    first = {"seq": 1, "role": "user", "content": "first"}
    last = {"seq": 2, "role": "assistant", "content": "last"}

    assert (await http.post(path, json={"messages": [first]})).json()["complete"] is False
    assert (await http.post(path, json={"messages": [{}], "complete": True})).status_code == 400
    assert (await http.post(path, json={"messages": []})).json()["complete"] is False
    assert (await http.post(path, json={"messages": [first, last], "complete": True})).json()["complete"] is True

    reopened = LocalHistory(gateway.settings.data_dir / "history.sqlite3")
    try:
        assert reopened.import_legacy([]) is True
        assert [row["upstream_seq"] for row in reopened.page()] == [1, 2]
    finally:
        reopened.close()


async def test_text_is_forwarded_while_voice_backend_work_holds_execution_lock(tmp_path):
    chat, sent = setup(tmp_path, [event("content_block_delta", {"delta": {"text": "结果"}})])
    await chat.client.turn_lock.acquire()
    try:
        await asyncio.wait_for(chat.submit(request({"message": "立即补充", "scene_id": "room"})), 1)
        await asyncio.wait_for(chat.job, 1)
        assert [body["message"] for body in sent] == ["立即补充"]
        assert chat.client.turn_lock.locked()
    finally:
        if chat.client.turn_lock.locked():
            chat.client.turn_lock.release()
        await chat.close()
        await chat.client.close()
        chat.history.close()


async def test_shared_stream_keeps_only_our_scene_and_separates_persisted_speech(tmp_path):
    chat, _ = setup(tmp_path, [
        event("meta", {"stream_id": "shared", "scene_id": "room"}),
        event("content_block_delta", {"delta": {"text": "过程"}}),
        event("meta", {"persisted": {"kind": "self", "seq": 100, "from": "act_talk"}}),
        event("content_block_delta", {"delta": {"text": "本机结果"}}),
        event("message_stop", {}),
        event("meta", {"continuation": True, "scene_id": "web"}),
        event("content_block_delta", {"delta": {"text": "网页秘密"}}),
        event("message_stop", {}),
        event("meta", {"continuation": True, "scene_id": None}),
        event("content_block_delta", {"delta": {"text": "Being 内部消息"}}),
        event("message_stop", {}),
    ])
    try:
        response = await chat.submit(request({"message": "请求", "scene_id": "room"}))
        stream = "".join([chunk async for chunk in response.body_iterator])
        assert "网页秘密" not in stream and "Being 内部消息" not in stream
        rows = chat.history.page()
        assert [row["content"] for row in rows] == ["请求", "过程", "本机结果"]
        assert rows[1]["from"] == "act_talk"
        assert all(row["scene_id"] == "room" for row in rows)
        assert chat.current["upstream_seq"] == 10
        assert [item["seq"] for item in chat.events] == [1, 2, 3, 4]
    finally:
        await chat.close()
        await chat.client.close()
        chat.history.close()


@pytest.mark.parametrize("restart", [False, True])
async def test_recovery_continues_saved_cursor_without_repeating_text_or_submission(tmp_path, restart):
    gate = asyncio.Event()
    chat, sent = setup(tmp_path, [event("meta", {"stream_id": "owned"}),
                                event("content_block_delta", {"delta": {"text": "前半"}})], gate)
    calls = []
    async def replay(req):
        calls.append(req)
        assert req.method == "GET"
        assert req.url.params["after"] == "1"
        return httpx.Response(200, json={"stream_id": "owned", "finished": True, "events": [
            {"seq": 1, "event": "content_block_delta", "data": {"delta": {"text": "前半"}}},
            {"seq": 2, "event": "content_block_delta", "data": {"delta": {"text": "后半"}}},
            {"seq": 3, "event": "message_stop", "data": {}},
        ]})
    try:
        await chat.submit(request({"message": "继续", "scene_id": "room"}))
        async with asyncio.timeout(2):
            while chat.current["text"] != "前半":
                await asyncio.sleep(.01)
        await chat.close()
        await chat.client.close()
        if restart:
            chat.history.close()
            chat = Chat(chat.client, LocalHistory(tmp_path / "history.sqlite3"))
        chat.client.http = httpx.AsyncClient(base_url="http://fixture/", transport=httpx.MockTransport(replay))
        chat.start()
        await chat.job
        assert len(sent) == len(calls) == 1
        assert [row["content"] for row in chat.history.page()] == ["继续", "前半后半"]
        assert [item["seq"] for item in chat.events] == [1, 2, 3]
        assert "".join(item["data"].get("delta", {}).get("text", "") for item in chat.events) == "前半后半"
        assert chat.current["finished"] and not chat.current.get("failed")
    finally:
        gate.set()
        await chat.close()
        await chat.client.close()
        chat.history.close()


@pytest.mark.parametrize("reply", [
    {"stream_id": "other-client", "finished": True, "events": []},
    {"stream_id": "owned", "finished": True, "events": [{"seq": 3, "event": "message_stop", "data": {}}]},
])
async def test_recovery_never_guesses_stream_ownership_or_skips_missing_events(tmp_path, reply):
    chat, _ = setup(tmp_path, [])
    chat.current = {"stream_id": "local", "upstream_id": "owned", "upstream_seq": 1,
                    "scene": {"scene_id": "room"}, "text": "已收到", "reply": 0, "sequence": 1}
    await chat.client.http.aclose()
    chat.client.http = httpx.AsyncClient(base_url="http://fixture/", transport=httpx.MockTransport(
        lambda req: httpx.Response(200, json=reply)))
    try:
        with pytest.raises(RuntimeError):
            await chat.recover()
        assert chat.current["text"] == "已收到"
    finally:
        await chat.client.close()
        chat.history.close()


def test_late_playback_keeps_original_scene_and_deleted_history_cannot_be_reimported(tmp_path):
    history = LocalHistory(tmp_path / "history.sqlite3")
    store = Store(tmp_path / "gateway.sqlite3")
    store.attach_history(history, {"scene_id": "original"})
    try:
        store.message("old", "assistant", "语音", id="reply", source="voice")
        store.scene = {"scene_id": "new"}
        store.message("old", "assistant", "语音", id="reply", playbackStatus="completed")
        store.message("other", "user", "保留")
        assert history.page(scene="original")[0]["playbackStatus"] == "completed"
        assert len(history.page(scene="new")) == 1
        old = store.history("old")[0]
        store.delete_conversation("old")
        history.capture(old, {"scene_id": "original"})
        store.message("old", "assistant", "迟到结果")
        assert [row["content"] for row in history.page()] == ["保留"]
    finally:
        store.close()
        history.close()


@pytest.mark.parametrize("status", [200, 202])
@pytest.mark.parametrize("metadata_first", [True, False])
async def test_local_supplement_preserves_splicing_and_the_new_stream_race(tmp_path, status, metadata_first):
    gate = asyncio.Event()
    posted, posts = [], 0
    spoken = []

    async def capture_feedback(_scene_id, feedback=None):
        if feedback and feedback["text"]:
            spoken.append(feedback["text"])

    class Original(httpx.AsyncByteStream):
        async def __aiter__(self):
            if not metadata_first:
                await gate.wait()
            yield event("meta", {"stream_id": "original", "scene_id": "first"})
            yield event("content_block_delta", {"delta": {"text": "第一段"}})
            await gate.wait()
            if status == 200:
                await asyncio.sleep(.02)
                yield event("content_block_delta", {"delta": {"text": "补全"}})
            yield event("message_stop", {})
            if status == 200:
                return
            yield event("meta", {"continuation": True, "scene_id": "second"})
            yield event("content_block_delta", {"delta": {"text": "补充结果"}})
            yield event("message_stop", {})
    async def respond(req):
        nonlocal posts
        if req.method == "GET":
            return httpx.Response(200, json={"stream_id": "original", "finished": False}) if posts else httpx.Response(204)
        assert req.url.path.endswith("/chat/stream")
        posted.append(json.loads(req.content)); posts += 1
        if posts == 1:
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Original())
        if status == 202:
            gate.set()
            return httpx.Response(202)
        gate.set()
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream([
            event("meta", {"stream_id": "new", "scene_id": "second"}),
            event("content_block_delta", {"delta": {"text": "补充结果"}}), event("message_stop", {}),
        ]))
    chat, _ = setup(tmp_path, [])
    chat.context_changed = capture_feedback
    await chat.client.http.aclose()
    chat.client.http = httpx.AsyncClient(base_url="http://fixture/", transport=httpx.MockTransport(respond))
    try:
        await chat.submit(request({"message": "第一条", "scene_id": "first"}))
        async with asyncio.timeout(2):
            while not posts or (metadata_first and chat.current["text"] != "第一段"):
                await asyncio.sleep(.01)
        body = {"message": "补充", "scene_id": "second", "client_request_id": "supplement"}
        response = await asyncio.wait_for(chat.submit(request(body)), 1)
        assert response.status_code == status
        await asyncio.gather(*chat.readers.values())
        rows = chat.history.page()
        assert [(row["content"], row["scene_id"]) for row in rows if row["role"] == "assistant"] == [
            ("第一段补全" if status == 200 else "第一段", "first"), ("补充结果", "second")]
        assert sorted(spoken) == sorted(row["content"] for row in rows if row["role"] == "assistant")
        assert (await chat.submit(request(body))).status_code == 202
        assert len(posted) == 2
        assert [row["content"] for row in rows if row["role"] == "user"] == ["第一条", "补充"]
    finally:
        gate.set()
        await chat.close()
        await chat.client.close()
        chat.history.close()


async def test_stream_id_cannot_change_and_provider_errors_preserve_partial_content(tmp_path):
    chat, _ = setup(tmp_path, [event("meta", {"stream_id": "owned"}),
        event("content_block_delta", {"delta": {"text": "部分内容"}}),
        event("error", {"error": "失败"}), event("message_stop", {})])
    try:
        await chat.submit(request({"message": "请求", "scene_id": "room"}))
        await chat.job
        assert chat.current["failed"]
        assert chat.history.page()[-1]["from"] == "partial"
        with pytest.raises(RuntimeError, match="identity"):
            chat.consume("meta", {"stream_id": "someone-else"})
        assert chat.current["upstream_id"] == "owned"
    finally:
        await chat.close()
        await chat.client.close()
        chat.history.close()


@pytest.mark.parametrize("initially_idle", [False, True])
async def test_first_202_is_forwarded_and_its_scene_reply_is_replayed_without_resubmitting(tmp_path, initially_idle):
    chat, _ = setup(tmp_path, [])
    posted = []
    polls = 0
    async def respond(req):
        nonlocal polls
        if req.method == "POST":
            posted.append(json.loads(req.content))
            return httpx.Response(202, json={"queued": True}, headers={"retry-after": "1"})
        polls += 1
        if initially_idle and polls == 1:
            return httpx.Response(204)
        return httpx.Response(200, json={"stream_id": "shared", "scene_id": "other", "finished": True, "events": [
            {"seq": 1, "event": "content_block_delta", "data": {"delta": {"text": "其他场景"}}},
            {"seq": 2, "event": "message_stop", "data": {}},
            {"seq": 3, "event": "meta", "data": {"continuation": True, "scene_id": "room"}},
            {"seq": 4, "event": "content_block_delta", "data": {"delta": {"text": "文字结果"}}},
            {"seq": 5, "event": "message_stop", "data": {"session_id": "session-1"}},
        ]})
    await chat.client.http.aclose()
    chat.client.http = httpx.AsyncClient(base_url="http://fixture/", transport=httpx.MockTransport(respond))
    body = {"message": "原样发送", "scene_id": "room", "session_id": "session-1", "client_request_id": "once"}
    chat.history.put("voice-old", {"role": "user", "content": "以前的语音", "scene_id": "room", "source": "voice-user"})
    try:
        response = await chat.submit(request(body))
        assert response.status_code == 202 and json.loads(response.body) == {"queued": True}
        assert response.headers["retry-after"] == "1"
        await asyncio.wait_for(chat.job, 2)
        assert posted == [{key: value for key, value in body.items() if key != "client_request_id"}]
        assert [row["content"] for row in chat.history.page()] == ["以前的语音", "原样发送", "文字结果"]
        assert chat.current["finished"] and not chat.current.get("failed")
        assert (await chat.submit(request(body))).status_code == 202
        assert len(posted) == 1
    finally:
        await chat.close(); await chat.client.close(); chat.history.close()


@pytest.mark.parametrize("second_status", [200, 202])
async def test_second_input_reaches_being_before_first_response_headers(tmp_path, second_status):
    headers, first_posted = asyncio.Event(), asyncio.Event()
    posted = []
    chat, _ = setup(tmp_path, [])
    async def respond(req):
        assert req.method == "POST"
        text = json.loads(req.content)["message"]
        posted.append(text)
        if text == "A":
            first_posted.set()
            await headers.wait()
            chunks = [event("meta", {"stream_id": "first", "scene_id": "room"}),
                      event("content_block_delta", {"delta": {"text": "A result"}}), event("message_stop", {})]
            if second_status == 202:
                chunks += [event("meta", {"continuation": True, "scene_id": "room"}),
                           event("content_block_delta", {"delta": {"text": "B result"}}), event("message_stop", {})]
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream(chunks))
        if second_status == 202:
            return httpx.Response(202, json={"queued": True})
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream([
            event("meta", {"stream_id": "second", "scene_id": "room"}),
            event("content_block_delta", {"delta": {"text": "B result"}}), event("message_stop", {})]))
    await chat.client.http.aclose()
    chat.client.http = httpx.AsyncClient(base_url="http://fixture/", transport=httpx.MockTransport(respond))
    first = asyncio.create_task(chat.submit(request({"message": "A", "scene_id": "room"})))
    try:
        await asyncio.wait_for(first_posted.wait(), 1)
        second = await asyncio.wait_for(chat.submit(request({"message": "B", "scene_id": "room"})), 1)
        assert second.status_code == second_status and posted == ["A", "B"]
        assert not first.done()
        headers.set()
        original = await first
        await asyncio.gather(*chat.readers.values())
        assert "A result" in "".join([part async for part in original.body_iterator])
        if second_status == 200:
            assert "B result" in "".join([part async for part in second.body_iterator])
            assert chat.current["upstream_id"] == "second"
        assert sorted(row["content"] for row in chat.history.page() if row["role"] == "assistant") == ["A result", "B result"]
    finally:
        headers.set(); await first
        await chat.close(); await chat.client.close(); chat.history.close()


@pytest.mark.parametrize("status", [400, 409, 429, 503])
async def test_text_http_errors_are_forwarded_and_explicit_retries_still_reach_being(tmp_path, status):
    chat, _ = setup(tmp_path, [])
    store = Store(tmp_path / "gateway.sqlite3")
    store.attach_history(chat.history, {"scene_id": "room"})
    posted = []
    async def respond(req):
        assert req.method == "POST"  # No active-stream preflight or waiting policy.
        posted.append(json.loads(req.content))
        return httpx.Response(status, json={"error": "original"}, headers={"retry-after": "2"})
    await chat.client.http.aclose()
    chat.client.http = httpx.AsyncClient(base_url="http://fixture/", transport=httpx.MockTransport(respond))
    try:
        for _ in range(2):
            response = await chat.submit(request({"message": "hello", "scene_id": "room", "client_request_id": "retry"}))
            assert response.status_code == status and json.loads(response.body) == {"error": "original"}
            assert response.headers["retry-after"] == "2"
        assert len(posted) == 2 and len(chat.history.page()) == 1
        assert snapshot(store, {}, "voice")["messages"][-1]["deliveryStatus"] == "failed"
    finally:
        store.close()
        await chat.close(); await chat.client.close(); chat.history.close()


async def test_uncertain_text_submission_is_not_repeated_or_reported_as_accepted(tmp_path):
    chat, _ = setup(tmp_path, [])
    store = Store(tmp_path / "gateway.sqlite3")
    store.attach_history(chat.history, {"scene_id": "room"})
    posted = []
    async def respond(req):
        posted.append(req)
        raise httpx.ReadError("connection lost")
    await chat.client.http.aclose()
    chat.client.http = httpx.AsyncClient(base_url="http://fixture/", transport=httpx.MockTransport(respond))
    try:
        for _ in range(2):
            response = await chat.submit(request({"message": "work", "scene_id": "room", "client_request_id": "uncertain"}))
            assert response.status_code == 502
        assert len(posted) == 1
        assert snapshot(store, {}, "voice")["messages"][-1]["deliveryStatus"] == "unknown"
    finally:
        store.close(); await chat.close(); await chat.client.close(); chat.history.close()


@pytest.mark.parametrize("status, receipt", [(202, {"ok": True}), (409, {"error": "busy"}), (200, {"ok": False}), (204, None)])
async def test_stop_preserves_receipt_and_only_finishes_after_confirmation(tmp_path, status, receipt):
    gate = asyncio.Event()
    chat, _ = setup(tmp_path, [event("meta", {"stream_id": "owned", "trace_id": "trace", "origin": "human"})], gate)
    try:
        response = await chat.submit(request({"message": "work", "scene_id": "room"}))
        first = await anext(response.body_iterator)
        assert '"trace_id": "trace"' in first and '"origin": "human"' in first
        async def stop(url, **kwargs):
            assert url == "api/stop" and kwargs["json"]["stream_id"] == "owned"
            return httpx.Response(status, json=receipt) if receipt is not None else httpx.Response(status)
        chat.client.http.post = stop
        stopped = await chat.stop(request({"stream_id": chat.current["stream_id"]}))
        assert stopped.status_code == status
        if receipt is not None:
            assert json.loads(stopped.body) == receipt
        assert chat.current["finished"] is (status == 204)
    finally:
        gate.set(); await chat.close(); await chat.client.close(); chat.history.close()


@pytest.mark.parametrize("voice", [False, True])
async def test_stop_serializes_with_new_admission_and_never_cancels_the_next_request(tmp_path, voice):
    stream_gate, cancel_started, confirm_cancel = asyncio.Event(), asyncio.Event(), asyncio.Event()
    chat, sent = setup(tmp_path, [event("meta", {"stream_id": "owned"})], stream_gate)
    post = chat.client.http.post
    async def cancel(url, **kwargs):
        assert url == "api/stop" and kwargs["json"]["stream_id"] == "owned"
        cancel_started.set()
        await confirm_cancel.wait()
        return await post(url, **kwargs)
    chat.client.http.post = cancel
    try:
        response = await chat.submit(request({"message": "先停止", "scene_id": "room"}))
        async with asyncio.timeout(2):
            while not chat.current.get("upstream_id"):
                await asyncio.sleep(.01)
        stream_id = chat.current["stream_id"]
        stopping = asyncio.create_task(chat.stop_voice(stream_id, "room", chat.request_order) if voice else
                                       chat.stop(request({"stream_id": stream_id})))
        await cancel_started.wait()
        sending = asyncio.create_task(chat.submit(request({"message": "下一条", "scene_id": "room"})))
        await asyncio.sleep(.01)
        assert not sending.done() and len(sent) == 1
        confirm_cancel.set()
        receipt = await stopping
        assert receipt["text_work"] == "stopped" if voice else json.loads(receipt.body)["ok"]
        events = "".join([line async for line in response.body_iterator])
        assert "event: stream_stopped" in events and stream_id in events
        assert (await sending).status_code == 200
        stream_gate.set()
        await chat.job
        assert len(sent) == 3 and not chat.current.get("failed")
    finally:
        confirm_cancel.set(); stream_gate.set()
        await chat.close()
        await chat.client.close()
        chat.history.close()


async def test_late_stop_receipt_cannot_stop_a_new_stream_already_in_flight(tmp_path):
    second_headers, second_posted, stop_started, stop_receipt, tail = [asyncio.Event() for _ in range(5)]
    chat, _ = setup(tmp_path, [])
    async def respond(req):
        body = json.loads(req.content)
        if req.url.path.endswith("/stop"):
            assert body["stream_id"] == "A"
            stop_started.set()
            await stop_receipt.wait()
            return httpx.Response(200, json={"ok": True, "stream_id": "A"})
        if body["message"] == "B":
            second_posted.set()
            await second_headers.wait()
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream([
            event("meta", {"stream_id": body["message"]}),
            event("content_block_delta", {"delta": {"text": body["message"] + " result"}}),
        ], tail))
    await chat.client.http.aclose()
    chat.client.http = httpx.AsyncClient(base_url="http://fixture/", transport=httpx.MockTransport(respond))
    second = stopping = None
    try:
        first = await chat.submit(request({"message": "A", "scene_id": "room"}))
        await anext(first.body_iterator)
        original = chat.current
        second = asyncio.create_task(chat.submit(request({"message": "B", "scene_id": "room"})))
        await second_posted.wait()
        stopping = asyncio.create_task(chat.stop(request({"stream_id": original["stream_id"]})))
        await stop_started.wait()
        second_headers.set()
        next_response = await asyncio.wait_for(second, 1)
        await anext(next_response.body_iterator)
        stop_receipt.set()
        receipt = await stopping
        assert receipt.status_code == 200 and json.loads(receipt.body)["stream_id"] == original["stream_id"]
        assert original["finished"] and not chat.current["finished"]
        assert chat.current["upstream_id"] == "B" and not chat.job.done()
    finally:
        second_headers.set(); stop_receipt.set(); tail.set()
        await asyncio.gather(*(job for job in (second, stopping) if job), return_exceptions=True)
        await chat.close(); await chat.client.close(); chat.history.close()
