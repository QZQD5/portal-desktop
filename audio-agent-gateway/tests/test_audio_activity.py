import random
import struct
from pathlib import Path

import pytest

from gateway.audio_activity import AudioActivity

# Synthetic TTS saying "停", PCM16 mono 16 kHz; no microphone recording.
SPEECH = (Path(__file__).parent / "fixtures/speech-stop.pcm").read_bytes()


@pytest.mark.parametrize("chunk_size", [86, 640, 1024, 8192])
def test_silero_detects_short_speech_after_silence_and_rearms(chunk_size):
    vad = AudioActivity()
    pcm = bytes(32000) + SPEECH + bytes(32000) + SPEECH + bytes(32000)
    starts = sum(vad.append(pcm[n:n + chunk_size]) for n in range(0, len(pcm), chunk_size))
    assert starts == 2
    assert not vad.speaking and vad.quiet_seconds > .8
    assert len(vad.pending) < 1024


def test_silero_ignores_silence_and_steady_background_noise():
    vad = AudioActivity()
    rng = random.Random(42)
    for _ in range(100):
        noise = struct.pack('<512h', *(int(rng.gauss(0, .02) * 32767) for _ in range(512)))
        assert not vad.append(noise)
    assert vad.quiet_seconds > 1
    vad.reset()
    assert not vad.speaking and not vad.pending
    assert not vad.append(bytes(64000))


def test_silero_session_state_is_isolated():
    first, second = AudioActivity(), AudioActivity()
    assert first.append(SPEECH)
    assert not second.append(bytes(32000))
    first.reset()
    assert first.quiet_seconds == 0
    assert first.append(SPEECH)
