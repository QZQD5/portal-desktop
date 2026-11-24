"""Join saved live probes to their Gateway metadata, offline. All relative times are milliseconds."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import wave


EXPECTED = {"ack": "KEEP", "continue": "KEEP", "stop": "CUT"}
ASR = "conversation.item.input_audio_transcription."
RECEIVED = {ASR + stage for stage in ("started", "delta", "text", "completed", "failed")} | {
    "response.canceled", "session.closed", "input_audio_buffer.committed",
}
LOG_DIR = Path(__file__).resolve().parents[1] / ".runtime/dev/logs"


def summarize(directory):
    runs = {}
    for path in sorted(directory.glob("eval-*.json")):
        run = json.loads(path.read_text(encoding="utf-8"))
        sid, case = run["session"], run["case"]
        if (case not in EXPECTED or path.stem != sid or
                not re.fullmatch(rf"eval-{case}-[0-9a-f]{{12}}", sid) or run["outcome"] != "observed"):
            raise ValueError(f"Expected a completed diagnostic probe: {path.name}")
        runs[sid] = run
    if not runs:
        raise ValueError("No completed probe reports found")

    # Discard unrelated lines before decoding their JSON or inspecting their messages.
    marker = re.compile(rb'realtime\.provider_(?:received|send_queued) session=('
                        + b"|".join(re.escape(s.encode()) for s in runs) + rb') ')
    messages = defaultdict(list)
    numbered = [(int(p.suffix[1:]), p) for p in LOG_DIR.glob("gateway.jsonl.*") if p.suffix[1:].isdigit()]
    paths = [p for _, p in sorted(numbered, reverse=True)] + [LOG_DIR / "gateway.jsonl"]
    for path in paths:
        with path.open("rb") as handle:
            for line in handle:
                hit = marker.search(line)
                if hit is None:
                    continue
                sid = hit[1].decode()
                log = json.loads(line)
                if log.get("component") != "gateway.voice" or log.get("schema") != "qwaudio.log/v1":
                    continue
                fields = log["message"].split()
                if fields[:2] not in (["realtime.provider_received", "session=" + sid],
                                      ["realtime.provider_send_queued", "session=" + sid]):
                    continue
                metadata = dict(v.split("=", 1) for v in fields[2:])
                kind = metadata["kind"]
                queued = fields[0] == "realtime.provider_send_queued"
                if (queued and kind == "response.cancel") or (not queued and kind in RECEIVED):
                    messages[sid].append((round(datetime.fromisoformat(log["time"]).timestamp() * 1000),
                                          int(metadata["epoch"]), queued, kind, metadata.get("item")))

    cases = []
    for sid, run in runs.items():
        trace = run["trace"]
        case = run["case"]
        start = next(e for e in trace if e["kind"] == "probe.start")
        target = run["target"]
        if start["rid"] != target:
            raise ValueError(f"Probe response ID mismatch: {case}")
        with wave.open(str(directory / "audio" / f"{case}.wav"), "rb") as audio:
            duration = round(audio.getnframes() * 1000 / audio.getframerate())
        end = duration + 5000
        at = lambda e: e["ms"] - start["ms"]
        observed = [e for e in trace if 0 <= at(e) <= end]
        clears = [at(e) for e in observed if e["kind"] == "playback.clear" and target in e["cleared"]]
        asr = [e for e in observed if e["kind"] in ("transcript.delta", "transcript.final")]
        closed_epochs = set()
        items = {}
        provider = []
        for unix_ms, epoch, queued, kind, item in messages[sid]:
            t = unix_ms - start["unix_ms"]
            event = {"ord": len(provider) + 1, "ms": t, "epoch": epoch, "kind": kind,
                     "source": "provider_send_queued" if queued else "provider_received",
                     "phase": "before_probe" if t < 0 else "in_window" if t <= end else "after_window",
                     "after_session_closed": epoch in closed_epochs}
            if item and item != "None":
                event["item"] = items.setdefault((epoch, item), f"item-{len(items) + 1}")
            if kind == "session.closed":
                closed_epochs.add(epoch)
            provider.append(event)
        cases.append({
            "case": case, "expected": EXPECTED[case], "target": target,
            "probe_start_utc": datetime.fromtimestamp(start["unix_ms"] / 1000, timezone.utc).isoformat(),
            "injected_clip_ms": duration, "nominal_observation_window_ms": [0, end],
            "last_client_event_ms": at(trace[-1]), "target_queued_at_probe_ms": start["queued_ms"],
            "target_already_audio_done_at_probe": start["already_audio_done"],
            "software_wait_to_target_clear_ms": clears[0] if clears else None,
            "observed_action": "CUT" if clears else "KEEP",
            "last_visible_probe_asr": {"ms": at(asr[-1]), "kind": asr[-1]["kind"],
                                       "text": asr[-1]["text"]} if asr else None,
            "old_audio_done_ms": [at(e) for e in trace if e["kind"] == "audio.done" and e["rid"] == target],
            "gateway_target_response_done": [
                {"ms": at(e), "status": e.get("status")} for e in trace
                if e["kind"] == "response.done" and e["rid"] == target
            ],
            "provider_metadata": provider, "provider_log_matched": bool(provider),
        })
    report = {
        "time_reference": "probe.start=0, first injected PCM chunk (may contain leading silence). Client uses "
                          "monotonic time; Gateway UTC logs align via the probe's wall-clock timestamp.",
        "observation": "Nominal end = WAV duration + 5000 ms. The probe records no exact disconnect timestamp. "
                       "KEEP means no clear of the target during this interval. Playback was simulated.",
        "provider_evidence": "ord is file order among retained metadata per probe. Queued cancel is local intent. "
                             "Received canceled is a provider event without a guaranteed target ID. Gateway "
                             "response.done is a client-visible normalized or synthetic event, not a raw provider log.",
        "cases": cases,
    }
    (directory / "summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    summarize(parser.parse_args().directory)
