"""Evaluate the shipped Silero onset and playback gate on local synthetic speech.

Windows: python scripts/eval_voice_acoustics.py (requires Huihui Desktop).
Use --fixture-only to measure just the repository's synthetic "停" recording.
Use --cases tests/fixtures/voice-turn-cases-1000.jsonl to synthesize the corpus.
All times are relative to the start of the file, in milliseconds of input audio.
"""

import argparse
import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import wave

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from gateway.audio_activity import AudioActivity, PLAYBACK_BARGE_FRAMES


PHRASES = {
    "backchannel": ["嗯", "对", "嗯嗯", "对对", "我在听"],
    "encourage_continuation": ["继续", "你继续", "嗯，你继续", "好，你接着说", "你说，我听着呢"],
    "stop": ["停", "等一下", "停一下", "先别说了", "等等，让我说一句"],
    "correction": ["不对", "改成明天", "我要改一下", "不是周三，是周四", "刚才说错了，改成下周五"],
}


def corpus_inputs(cases):
    content = cases.read_bytes()
    corpus_sha = hashlib.sha256(content).hexdigest()
    items = []
    for line in content.splitlines():
        case = json.loads(line)
        phrase = case["transcript"]
        reason = None
        if not phrase.strip():
            reason = "Empty transcript; the described sound has no speech to synthesize."
        elif case["family"] == "只有噪声或非词发声":
            reason = "Nonlexical sound is represented by a hypothetical transcript; TTS cannot recreate the event."
        text_sha = hashlib.sha256(phrase.encode("utf-8")).hexdigest()
        items.append({
            "id": case["id"], "category": case["category"], "family": case["family"],
            "text": phrase, "expected_action": case["expected_action"], "source_cases_sha256": corpus_sha,
            "source_case_sha256": hashlib.sha256(line).hexdigest(), "text_sha256": text_sha,
            "file": None if reason else f"{text_sha}.wav", "skip_reason": reason,
            "rate": None if reason else 0, "source": None if reason else "Microsoft Huihui Desktop SAPI",
        })
    return items


def measure(item, pcm):
    vad = AudioActivity()
    # Warm the state with 20 silent frames. Include a silent tail so the last
    # partial frame of the recording is processed just as it is in a live stream.
    vad.append(bytes(20 * 1024))
    first_onset = first_gate = None
    max_voiced_frames = 0
    signal = pcm + bytes(12 * 1024)
    for offset in range(0, len(signal) - 1023, 1024):
        frame_end_ms = (offset + 1024) // 32
        started = vad.append(signal[offset:offset + 1024])
        if started and first_onset is None:
            first_onset = frame_end_ms
        if vad.playback_barge() and first_gate is None:
            first_gate = frame_end_ms
        max_voiced_frames = max(max_voiced_frames, vad.voiced_frames)
    return {
        **item, "sample_rate_hz": 16000, "pcm_duration_ms": len(pcm) / 32,
        "pcm_sha256": hashlib.sha256(pcm).hexdigest(), "first_onset_ms": first_onset,
        "first_playback_gate_ms": first_gate, "max_voiced_frames": max_voiced_frames,
        "required_playback_voiced_frames": PLAYBACK_BARGE_FRAMES,
        "eval_layer": "local_silero_vad_only",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--fixture-only", action="store_true")
    mode.add_argument("--prepare-inputs", type=Path, help="Write an input manifest for standalone SAPI generation")
    mode.add_argument("--audio-dir", type=Path, help="Evaluate the already synthesized inputs.json and WAVs")
    parser.add_argument("--cases", type=Path, help="JSONL cases with id, category, family, transcript and expected_action")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.cases and args.fixture_only:
        parser.error("--cases and --fixture-only select different inputs")
    corpus = bool(args.cases)
    rows = []
    with tempfile.TemporaryDirectory() as temp:
        directory = (args.prepare_inputs or args.audio_dir or Path(temp)).resolve()
        if not args.fixture_only:
            manifest = directory / "inputs.json"
            if not args.audio_dir:
                directory.mkdir(exist_ok=True, parents=True)
                items = corpus_inputs(args.cases) if corpus else [
                    {"file": f"{category}-{index}-rate{rate}.wav", "category": category,
                     "text": phrase, "rate": rate, "source": "Microsoft Huihui Desktop SAPI"}
                    for category, phrases in PHRASES.items()
                    for index, phrase in enumerate(phrases, 1) for rate in (0, 3)
                ]
                manifest.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
                if args.prepare_inputs:
                    return
                subprocess.run(["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
                                "-File", str(Path(__file__).with_name("eval_voice_synthesize.ps1")),
                                "-OutputDirectory", str(directory)], check=True)
            items = json.loads(manifest.read_text(encoding="utf-8"))
            if args.audio_dir and args.cases and items != corpus_inputs(args.cases):
                raise ValueError("Prepared inputs differ from --cases; synthesize the selected corpus first.")
            corpus = bool(items and "id" in items[0])
            measured = {}
            for index, item in enumerate(items, 1):
                if item.get("skip_reason"):
                    rows.append({**item, "eval_status": "skipped", "first_onset_ms": None,
                                 "first_playback_gate_ms": None, "pcm_sha256": None,
                                 "eval_layer": "local_silero_vad_only"})
                    continue
                name = item["file"]
                if name not in measured:
                    with wave.open(str(directory / name), "rb") as wav:
                        if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) != (1, 2, 16000):
                            raise ValueError(f"Expected 16 kHz mono PCM16 for {name}")
                        measured[name] = measure({}, wav.readframes(wav.getnframes()))
                rows.append({**item, **measured[name], "eval_status": "measured"})
                if corpus and index % 200 == 0:
                    print(f"Silero: {index}/{len(items)} cases, {len(measured)} distinct clips measured", flush=True)
        if not corpus:
            fixture = ROOT / "tests/fixtures/speech-stop.pcm"
            rows.append(measure({"file": fixture.name, "text": "停", "category": "stop", "rate": None,
                                 "source": "repository TTS fixture"}, fixture.read_bytes()))
    provenance = {
        "torch_version": version("torch"),
        "detector_sha256": hashlib.sha256((ROOT / "gateway/audio_activity.py").read_bytes()).hexdigest(),
        "weights_sha256": hashlib.sha256((ROOT / "gateway/resources/vad/silero_vad.jit").read_bytes()).hexdigest(),
    }
    for row in rows:
        row.update(provenance)
    output = args.output or ROOT / "test-results/voice-turn-eval" / ("corpus-acoustics.jsonl" if corpus else "acoustics.jsonl")
    output.parent.mkdir(exist_ok=True, parents=True)
    output.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
    print(f"{len(rows)} result rows -> {output}")


if __name__ == "__main__":
    main()
