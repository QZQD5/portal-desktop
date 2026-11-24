"""Three small live voice probes through an already configured local development Gateway."""

import argparse
import asyncio
import base64
import json
import time
import uuid
import wave
from pathlib import Path
from urllib.request import urlopen

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


def pcm(file):
    with wave.open(str(file), "rb") as stream:
        if (stream.getframerate(), stream.getnchannels(), stream.getsampwidth()) != (
            16000,
            1,
            2,
        ):
            raise ValueError(f"Expected 16 kHz mono PCM16: {file.name}")
        return stream.readframes(stream.getnframes())


async def probe(port, directory, name):
    prompt, phrase = (pcm(directory / "audio" / f"{f}.wav") for f in ("prompt", name))
    origin = f"http://127.0.0.1:{port}"
    for _ in range(10):
        with urlopen(origin + "/api/health", timeout=5) as resp:
            health = json.load(resp)
        if not health["voiceConfigured"]:
            raise RuntimeError("Voice is unconfigured")
        if not health["voiceClients"]["activeOwners"]:
            break
        await asyncio.sleep(1)
    else:
        print(
            "The current voice owner is busy. Stopping without taking over.", flush=True
        )
        return "occupied"
    sid = f"eval-{name}-{uuid.uuid4().hex[:12]}"
    trace = []
    start = time.monotonic()

    def note(kind, **fields):
        trace.append(
            dict(
                ms=round((time.monotonic() - start) * 1000),
                unix_ms=round(time.time() * 1000),
                kind=kind,
                **fields,
            )
        )

    report = {"case": name, "session": sid, "trace": trace, "outcome": "incomplete"}
    blocks, done = {}, set()
    ready_at = prompt_at = probe_at = None
    target = None
    try:
        async with connect(
            origin.replace("http:", "ws:") + "/api/realtime?sessionId=" + sid,
            proxy=None,
            max_size=2**21,
            close_timeout=2,
        ) as ws:

            async def send(kind, **fields):
                await ws.send(
                    json.dumps(dict(type=kind, event_id=uuid.uuid4().hex, **fields))
                )

            await send(
                "session.hello",
                protocol={"min": "7.0.0", "max": "7.0.0"},
                client={
                    "type": "cli",
                    "instance_id": uuid.uuid4().hex,
                    "label": "Voice diagnostics",
                },
                capabilities=["input.audio", "playback.receipts", "session.heartbeat"],
                locale="zh-CN",
                time_zone="Asia/Shanghai",
                connection={
                    "voice_enabled": True,
                    "input_enabled": True,
                    "output_enabled": True,
                    "text_only": False,
                },
            )

            async def receive():
                nonlocal ready_at
                async for raw in ws:
                    event = json.loads(raw)
                    kind, rid = event["type"], event.get("responseId")
                    if kind == "session.ping":
                        await send("session.pong", request_event_id=event["event_id"])
                    elif kind == "voice.ready" and ready_at is None:
                        ready_at = time.monotonic()
                        note(kind)
                    elif kind == "audio.delta":
                        if rid not in blocks:
                            blocks[rid] = dict(
                                duration=0.0,
                                started=None,
                                cleared=False,
                                ended=False,
                                packets=0,
                            )
                            note("audio.first", rid=rid)
                        item = blocks[rid]
                        item["packets"] += 1
                        item["duration"] += len(base64.b64decode(event["audio"])) / (
                            2 * event.get("sampleRate", 24000)
                        )
                    elif kind == "audio.done":
                        done.add(rid)
                        note(kind, rid=rid)
                    elif kind == "playback.clear":
                        clearing = [
                            r
                            for r, b in blocks.items()
                            if not (b["ended"] or b["cleared"])
                        ]
                        note(kind, reason=event.get("reason"), cleared=clearing)
                        for r in clearing:
                            blocks[r]["cleared"] = True
                            await send(
                                "playback.cancelled",
                                responseId=r,
                                reason="user_interruption",
                            )
                    elif (
                        kind in ("transcript.delta", "transcript.final")
                        and event.get("role") == "user"
                    ):
                        note(
                            kind,
                            text=event.get("content", ""),
                            turn=event.get("turnId"),
                        )
                    elif kind in (
                        "response.started",
                        "response.done",
                        "response.interrupted",
                    ):
                        note(
                            kind,
                            rid=rid,
                            origin=event.get("origin"),
                            status=event.get("status"),
                        )
                    elif kind == "tool.call":
                        note("tool.call")
                    elif kind == "error":
                        note(kind, code=(event.get("error") or {}).get("code"))

            receiver = asyncio.create_task(receive())
            try:
                frame = 1280
                cursor = 0
                phrase_cursor = 0
                tick = time.monotonic()
                while time.monotonic() - start < 70:
                    if receiver.done():
                        await receiver
                        report["outcome"] = "connection_closed"
                        break
                    now = time.monotonic()
                    if ready_at is None:
                        await asyncio.sleep(0.04)
                        tick = time.monotonic()
                        continue
                    if now - ready_at > 32 and probe_at is None:
                        report["outcome"] = "no_prompt_playback"
                        break
                    for r, b in blocks.items():
                        if b["cleared"] or b["ended"]:
                            continue
                        if b["started"] is None and (b["duration"] >= 0.4 or r in done):
                            b["started"] = now
                            await send("playback.started", responseId=r)
                            note("playback.started.virtual", rid=r)
                        if (
                            b["started"] is not None
                            and r in done
                            and now - b["started"] >= b["duration"]
                        ):
                            b["ended"] = True
                            await send("playback.ended", responseId=r)
                            note("playback.ended.virtual", rid=r)
                        if (
                            probe_at is None
                            and prompt_at is not None
                            and cursor >= len(prompt)
                            and b["started"] is not None
                            and not b["ended"]
                            and now - b["started"] >= 0.36
                            and b["duration"] - (now - b["started"]) > 0.12
                        ):
                            probe_at = now
                            target = r
                            note(
                                "probe.start",
                                rid=r,
                                already_audio_done=r in done,
                                queued_ms=round(
                                    1000 * (b["duration"] - (now - b["started"]))
                                ),
                            )
                    chunk = bytes(frame)
                    if probe_at is not None and phrase_cursor < len(phrase):
                        chunk = phrase[phrase_cursor : phrase_cursor + frame].ljust(
                            frame, b"\x00"
                        )
                        phrase_cursor += frame
                    elif now - ready_at >= 0.48 and cursor < len(prompt):
                        if prompt_at is None:
                            prompt_at = now
                            note("prompt.start")
                        chunk = prompt[cursor : cursor + frame].ljust(frame, b"\x00")
                        cursor += frame
                    await send(
                        "input_audio_buffer.append",
                        audio=base64.b64encode(chunk).decode("ascii"),
                    )
                    if (
                        probe_at is not None
                        and now - probe_at >= len(phrase) / 32000 + 5
                    ):
                        note("probe.window_end")
                        report["outcome"] = "observed"
                        report["target"] = target
                        break
                    tick += 0.04
                    await asyncio.sleep(max(0, tick - time.monotonic()))
            finally:
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
    except ConnectionClosed as e:
        report["outcome"] = f"closed_{e.rcvd.code if e.rcvd else 'unknown'}"
    finally:
        report["audio"] = {
            r: {
                "packets": b["packets"],
                "duration_ms": round(1000 * b["duration"]),
                "cleared": b["cleared"],
                "ended": b["ended"],
            }
            for r, b in blocks.items()
        }
        (directory / f"{sid}.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    print(f"{name}: {report['outcome']} ({sid})", flush=True)
    return report["outcome"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--port", type=int, default=3102)
    parser.add_argument(
        "--cases",
        nargs="+",
        choices=["ack", "continue", "stop"],
        default=["ack", "continue", "stop"],
    )
    args = parser.parse_args()
    for name in args.cases:
        if asyncio.run(probe(args.port, args.directory, name)) != "observed":
            break
