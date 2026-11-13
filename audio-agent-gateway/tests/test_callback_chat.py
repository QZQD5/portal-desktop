import asyncio
import json

import httpx
import pytest

from gateway.being import BeingClient
from gateway.chat import Chat
from gateway.config import Settings
from gateway.local_history import LocalHistory
from test_local_chat import Stream, event, request


def callback(stream="callback-one", text="后台结果", scene=None, finished=True):
    return {"stream_id": stream, "origin": "callback", "scene_id": scene, "finished": finished,
            "next_seq": 3, "events": [
                {"seq": 1, "event": "content_block_delta", "data": {"delta": {"text": text}}},
                {"seq": 2, "event": "message_stop", "data": {}},
            ]}


def make_chat(tmp_path, respond, feedback=None):
    client = BeingClient(Settings(being_endpoint="http://127.0.0.1/being", being_token="fixture",
                                 being_scene={"scene_id": "room"}), transport=httpx.MockTransport(respond))
    return Chat(client, LocalHistory(tmp_path / "history.sqlite3"), context_changed=feedback)


async def close(chat):
    await chat.close()
    await chat.client.close()
    chat.history.close()


async def test_finished_global_callback_is_archived_once_and_survives_restart(tmp_path):
    calls, feedback = [], []

    async def respond(req):
        calls.append((req.method, req.url.path))
        assert req.method == "GET"
        return httpx.Response(200, json=callback())

    async def updated(scene, value):
        if value and value.get("text"):
            feedback.append((scene, value))

    chat = make_chat(tmp_path, respond, updated)
    try:
        await chat.discover_callback()
        await chat.job
        assert chat.current["meta"]["origin"] == "callback" and chat.current["finished"]
        assert chat.current["scene"]["scene_id"] is None
        assert [row["content"] for row in chat.history.page()] == ["后台结果"]
        assert chat.history.page()[0]["scene_id"] == ""
        assert feedback[0][0] is None and feedback[0][1]["final"]
        await chat.discover_callback()
        assert len(chat.history.page()) == len(feedback) == 1
    finally:
        await close(chat)
    restarted = make_chat(tmp_path, respond)
    try:
        await restarted.discover_callback()
        assert restarted.job is None
        assert len(restarted.history.page()) == 1
        assert all(method == "GET" for method, _ in calls)
    finally:
        await close(restarted)


async def test_callback_continuations_keep_scenes_and_do_not_import_other_clients(tmp_path):
    active = callback(scene="web", finished=False)
    active["events"] = [{"seq": 1, "event": "meta", "data": {"stream_id": "callback-one"}}] + [
        {**item, "seq": item["seq"] + 1} for item in active["events"]]
    active["events"] += [
        {"seq": 4, "event": "meta", "data": {"scene_id": "room", "continuation": True}},
        {"seq": 5, "event": "content_block_delta", "data": {"delta": {"text": "本机场景结果"}}},
        {"seq": 6, "event": "message_stop", "data": {}},
    ]
    active["next_seq"] = 7
    chat = make_chat(tmp_path, lambda req: httpx.Response(200, json=active))
    try:
        await chat.discover_callback()
        async with asyncio.timeout(2):
            while chat.current["upstream_seq"] < 6:
                await asyncio.sleep(.01)
        assert not chat.current["finished"]  # message_stop is only a reply boundary.
        assert chat.current["meta"]["origin"] == "callback"
        active["events"] += [
            {"seq": 7, "event": "meta", "data": {"scene_id": None, "continuation": True}},
            {"seq": 8, "event": "content_block_delta", "data": {"delta": {"text": "共享完成说明"}}},
            {"seq": 9, "event": "message_stop", "data": {}},
        ]
        active.update(finished=True, next_seq=10)
        await asyncio.wait_for(chat.job, 2)
        assert [(row["scene_id"], row["content"]) for row in chat.history.page()] == [
            ("room", "本机场景结果"), ("", "共享完成说明")]
        assert "后台结果" not in json.dumps(chat.events, ensure_ascii=False)
    finally:
        await close(chat)


@pytest.mark.parametrize("origin", ["human", "beating", None])
async def test_discovery_does_not_adopt_unrelated_streams(tmp_path, origin):
    active = {**callback(), "origin": origin}
    chat = make_chat(tmp_path, lambda req: httpx.Response(200, json=active))
    try:
        await chat.discover_callback()
        assert chat.current is None and not chat.history.page()
    finally:
        await close(chat)


async def test_slow_discovery_cannot_replace_a_new_human_request(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(req):
        if req.method == "GET":
            entered.set()
            await release.wait()
            return httpx.Response(200, json=callback())
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream([
            event("meta", {"stream_id": "human"}),
            event("content_block_delta", {"delta": {"text": "新请求结果"}}),
            event("message_stop", {}),
        ]))

    chat = make_chat(tmp_path, respond)
    discovering = asyncio.create_task(chat.discover_callback())
    try:
        await entered.wait()
        response = await chat.submit(request({"message": "新请求", "scene_id": "room"}))
        async for _ in response.body_iterator:
            pass
        release.set()
        await discovering
        await asyncio.gather(*list(chat.readers.values()))
        assert chat.current["upstream_id"] == "human"
        assert [row["content"] for row in chat.history.page()] == ["新请求", "新请求结果", "后台结果"]
    finally:
        release.set()
        await asyncio.gather(discovering, return_exceptions=True)
        await close(chat)


async def test_callback_recovery_resumes_cursor_and_replaces_partial_history(tmp_path):
    active = callback(text="前半", finished=False)
    active["events"] = active["events"][:1]
    active["next_seq"] = 2
    calls = []

    def respond(req):
        calls.append(req)
        after = int(req.url.params.get("after", 0))
        return httpx.Response(200, json={**active, "events": [e for e in active["events"] if e["seq"] > after]})

    chat = make_chat(tmp_path, respond)
    await chat.discover_callback()
    async with asyncio.timeout(2):
        while chat.current["upstream_seq"] != 1:
            await asyncio.sleep(.01)
    await close(chat)
    active["events"] += [
        {"seq": 2, "event": "content_block_delta", "data": {"delta": {"text": "后半"}}},
        {"seq": 3, "event": "message_stop", "data": {}},
    ]
    active.update(finished=True, next_seq=4)
    restarted = make_chat(tmp_path, respond)
    try:
        restarted.start()
        await asyncio.wait_for(restarted.job, 2)
        assert calls[-1].url.params["after"] == "1"
        assert [row["content"] for row in restarted.history.page()] == ["前半后半"]
        assert restarted.current["finished"] and not restarted.current.get("failed")
    finally:
        await close(restarted)


def test_shared_callback_context_excludes_other_scenes_and_legacy_global_messages(tmp_path):
    history = LocalHistory(tmp_path / "history.sqlite3")
    try:
        for identity, scene, source in [("own", "room", "text-being"),
                                        ("shared", "", "portal-callback"),
                                        ("other", "other-room", "portal-callback"),
                                        ("legacy", "", "legacy-desktop")]:
            history.put(identity, {"role": "assistant", "content": identity,
                                   "scene_id": scene, "source": source})
        assert [row["content"] for row in history.page(scene="room", include_callbacks=True)] == ["own", "shared"]
        assert [row["content"] for row in history.page(scene="room")] == ["own"]
    finally:
        history.close()


async def test_callbacks_are_archived_while_human_stream_is_busy_without_replacing_it(tmp_path):
    release = asyncio.Event()
    active = callback(text="first result")

    async def respond(req):
        if req.method == "GET":
            return httpx.Response(200, json=active)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream([
            event("meta", {"stream_id": "human", "scene_id": "room"})], release))

    chat = make_chat(tmp_path, respond)
    try:
        response = await chat.submit(request({"message": "keep working", "scene_id": "room"}))
        human, human_job = chat.current, chat.job
        for text in ("first result", "second result"):
            active = callback(stream=text, text=text)
            await chat.discover_callback()
            jobs = [job for job in chat.readers.values() if job is not human_job]
            await asyncio.gather(*jobs)
            await chat.discover_callback()
            assert chat.current is human and not human["finished"]
        assert [row["content"] for row in chat.history.page()] == ["keep working", "first result", "second result"]
        release.set()
        async for _ in response.body_iterator:
            pass
    finally:
        release.set()
        await close(chat)
