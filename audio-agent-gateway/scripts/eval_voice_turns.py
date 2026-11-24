"""Replay native ASR events against the Gateway's live-turn logic, entirely offline.

Run with the Gateway Python environment: eval_voice_turns.py CASES.jsonl -o RESULTS.jsonl
Profiles script the ordering of detected speech and ASR. They do not benchmark
microphones, the Silero model's acoustic accuracy, cloud inference or latency.
"""
import argparse
import asyncio
import base64
import json
import sys
from contextlib import nullcontext
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gateway.app import Gateway
from gateway.config import Settings
from gateway.voice import VoiceSession


class VoicedFrame:
    """Substitute a deterministic score for the acoustic network, keeping production VAD hysteresis."""
    def reset_states(self):
        pass

    def __call__(self, samples, rate):
        return torch.tensor(0.99)


async def evaluate(gateway, case):
    case_id = str(case["id"])
    profile = case["input_profile"]
    gateway.settings.being_scene["scene_id"] = case_id
    session = VoiceSession(gateway, None, case_id, {"capabilities": ["input.audio"]})
    # Model the connected state while replacing only the two wire transports.
    # No provider loop, model inference, task worker or websocket is started.
    first_clear = None
    trigger = "setup"
    delta_index = None

    async def record(event):
        nonlocal first_clear
        if event["type"] == "playback.clear" and first_clear is None:
            first_clear = (trigger, delta_index, dict(event))

    session.emit = record

    async def discard_upstream_packet(event):
        pass

    session.upstream_send = discard_upstream_packet
    session.ready.set()

    async def deliver(event):
        for normalized in session.protocol.normalize(event):
            await session.provider_event(normalized)

    async def local_frames(count):
        # 512 mono samples/frame. Only the injected score carries acoustic evidence.
        await session.append_audio(base64.b64encode(b"\x01\x00" * (512 * count)).decode("ascii"))

    reply_id = f"playing-{case_id}"
    asr = "conversation.item.input_audio_transcription."
    try:
        await deliver({"type": "response.output_text.delta", "response_id": reply_id, "delta": case["assistant_speech"]})
        await deliver({"type": "response.output_audio.delta", "response_id": reply_id, "delta": "AAAAAA=="})
        await session.playback_started(reply_id)
        if reply_id not in session.playing:
            raise RuntimeError("The assistant reply did not reach playback")

        if profile != "cloud_first":
            trigger = "local_sustained" if profile == "sustained" else "local_short"
            await local_frames(32 if profile == "sustained" else 4)
        trigger = "asr_start"
        await deliver({"type": asr + "started", "item_id": "input"})
        if profile == "cloud_first":
            trigger = "local_short"
            await local_frames(4)
        for delta_index, text in enumerate(case["asr_deltas"]):
            trigger = "asr_delta"
            await deliver({"type": asr + "delta", "item_id": "input", "delta": text})
        trigger, delta_index = "asr_final", None
        await deliver({"type": asr + "completed", "item_id": "input", "text": case["transcript"]})
        actual = "CUT" if first_clear else "KEEP"
        return {
            "id": case["id"], "input_profile": profile, "category": case["category"],
            "expected_action": case["expected_action"], "actual_action": actual,
            "correct": actual == case["expected_action"],
            "first_clear_trigger": first_clear[0] if first_clear else None,
            "first_clear_delta_index": first_clear[1] if first_clear else None,
            "first_clear_event": first_clear[2] if first_clear else None,
        }
    finally:
        await session.close()


async def run(input_path, output_path, profile=None):
    if output_path and input_path.resolve() == output_path.resolve():
        raise ValueError("Input and output must be different files")
    if output_path is None:
        sys.stdout.reconfigure(encoding="utf-8")
    seen = set()
    def reject_request(request):
        raise RuntimeError("Offline replay attempted an HTTP request")

    offline_client = partial(httpx.AsyncClient, trust_env=False, transport=httpx.MockTransport(reject_request))
    with (TemporaryDirectory() as directory,
          patch("gateway.audio_activity.preload_model", return_value=VoicedFrame()),
          patch("gateway.being.httpx.AsyncClient", offline_client)):
        gateway = Gateway(Settings(data_dir=Path(directory), being_endpoint="http://127.0.0.1:1/eval",
                                   being_token="offline-fixture", being_scene={"scene_id": "eval"},
                                   doubao_api_key="", backend_enabled=False))
        try:
            with input_path.open(encoding="utf-8-sig") as source, (
                output_path.open("w", encoding="utf-8") if output_path else nullcontext(sys.stdout)
            ) as out:
                for line_number, line in enumerate(source, 1):
                    case = json.loads(line)
                    if isinstance(case, dict):
                        # Older standalone probes use context solely as the spoken line.
                        case.setdefault("assistant_speech", case.get("context"))
                    if (not isinstance(case, dict) or not isinstance(case.get("id"), (str, int))
                            or isinstance(case["id"], bool) or not str(case["id"]) or str(case["id"]) in seen
                            or case.get("input_profile") not in ("short", "sustained", "cloud_first")
                            or case.get("expected_action") not in ("KEEP", "CUT")
                            or not all(isinstance(case.get(k), str) for k in ("assistant_speech", "transcript", "category"))
                            or not isinstance(case.get("asr_deltas"), list)
                            or not all(isinstance(text, str) for text in case["asr_deltas"])):
                        raise ValueError(f"Invalid case or duplicate id at line {line_number}")
                    seen.add(str(case["id"]))
                    if profile is not None:
                        case["input_profile"] = profile
                    result = await evaluate(gateway, case)
                    out.write(json.dumps(result, ensure_ascii=False) + "\n")
        finally:
            await gateway.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("-o", "--output", type=Path, help="UTF-8 JSONL output (default: stdout)")
    parser.add_argument("--profile", choices=("short", "sustained", "cloud_first"),
                        help="Override the event ordering for every row")
    args = parser.parse_args()
    asyncio.run(run(args.input, args.output, args.profile))
