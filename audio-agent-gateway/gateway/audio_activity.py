"""Local speech onset only; cloud ASR still owns utterance boundaries."""

# 32 ms frames. Playback stays up through a cough; a sustained utterance can still cut it.
PLAYBACK_BARGE_FRAMES = 28

from copy import deepcopy
from functools import cache
from pathlib import Path

import torch


@cache
def preload_model():
    torch.set_num_threads(1)
    model = torch.jit.load(str(Path(__file__).parent / "resources/vad/silero_vad.jit"), map_location="cpu")
    model.eval()
    with torch.inference_mode():
        for _ in range(3):
            model(torch.zeros(512), 16000)
    model.reset_states()
    return model


class AudioActivity:
    def __init__(self):
        # Silero stores recurrent state in the module, so each session needs its own copy.
        self.model = deepcopy(preload_model())
        self.reset()

    def reset(self):
        self.model.reset_states()
        self.pending = bytearray()
        self.voiced_frames = 0
        self.quiet_frames = 0
        self.speaking = False

    @torch.inference_mode()
    def append(self, pcm):
        self.pending.extend(pcm)
        started = False
        # Silero consumes 512 samples (32 ms); preserve arbitrary browser packet boundaries.
        while len(self.pending) >= 1024:
            frame = self.pending[:1024]
            del self.pending[:1024]
            samples = torch.frombuffer(frame, dtype=torch.int16).float() / 32768
            probability = self.model(samples, 16000).item()
            if probability >= .6:
                self.quiet_frames = 0
                self.voiced_frames += 1
                if self.voiced_frames >= 3 and not self.speaking:
                    self.speaking = True
                    started = True
            elif probability < .45:
                self.voiced_frames = 0
                self.quiet_frames += 1
                if self.quiet_frames >= 10:
                    self.speaking = False
                # Keep long idle noise from contaminating the next speech onset.
                if self.quiet_frames % 16 == 0:
                    self.model.reset_states()
            else:
                self.voiced_frames = 0
                self.quiet_frames = 0
        return started

    def playback_barge(self):
        return self.speaking and self.voiced_frames >= PLAYBACK_BARGE_FRAMES

    @property
    def quiet_seconds(self):
        return self.quiet_frames * .032
