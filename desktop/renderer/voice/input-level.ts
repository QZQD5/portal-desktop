/** Local microphone feedback only; no extra audio is sent to the gateway. */
export class MicrophoneInputLevel {
  private energy = 0;
  private count = 0;
  private level = 0;

  add(samples: Float32Array) {
    for (const sample of samples) this.energy += sample * sample;
    this.count += samples.length;
  }

  read() {
    const rms = this.count ? Math.sqrt(this.energy / this.count) : 0;
    // Suppress low background noise and make ordinary speech visible.
    const target = Math.min(1, Math.max(0, (rms - 0.008) / 0.12));
    this.level = target >= this.level ? target : this.level * 0.45;
    if (this.level < 0.02) this.level = 0;
    this.energy = 0; this.count = 0;
    return Math.round(this.level * 100) / 100;
  }

  reset() { this.energy = 0; this.count = 0; this.level = 0; }
}
