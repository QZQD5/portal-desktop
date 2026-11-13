import type { VoiceAPI, VoiceEvent } from '../../shared/voice';
import { createStreamingResampler, pcmBase64, decodePcm, audioSchedulingLeadSeconds, createPcmPlaybackQueue, type PcmPlaybackItem } from './audio.js';
import { createMicrophoneAudioWorkletNode } from './microphone-audio-worklet.js';
import { createMicrophoneCaptureLifecycle } from './microphone-capture.js';
import { createPlaybackOutputFollow } from './playback-lifecycle.js';
import { VoicePresentationStream } from './presentation';
import { MicrophoneInputLevel } from './input-level';

interface Playback {
  sources: Set<AudioBufferSourceNode>; resampler: ReturnType<typeof createStreamingResampler>;
  done: boolean; started: boolean; timer?: ReturnType<typeof setTimeout>;
}

export class VoiceSession {
  private context?: AudioContext;
  private capture?: ReturnType<typeof createMicrophoneCaptureLifecycle>;
  private follow?: ReturnType<typeof createPlaybackOutputFollow>;
  private unsubscribe?: () => void;
  private playing = new Map<string, Playback>();
  // The local Gateway still delivers cloud audio with network jitter.
  private queue = createPcmPlaybackQueue({ remote: true, onFlush: items => items.forEach(item => this.schedule(item)) });
  private suppressed = new Set<string>();
  private nextTime = 0;
  private queuedBytes = 0;
  private generation = 0;
  private ready = false;
  private muted = false;
  private inputLevel = new MicrophoneInputLevel();
  private lastInputLevel = 0;
  private presentation: VoicePresentationStream;
  private presentationTimer?: ReturnType<typeof setInterval>;
  constructor(private api: VoiceAPI, private event: (event: VoiceEvent) => void, sceneId = '') {
    this.presentation = new VoicePresentationStream(sceneId, presentation => this.event({ type: 'desktop.presentation', presentation }));
  }

  async start() {
    const generation = ++this.generation;
    this.context = new AudioContext();
    await this.context.resume();
    this.presentation.tick(this.context.currentTime);
    this.presentationTimer = setInterval(() => {
      this.presentation.tick(this.context?.currentTime || 0);
      this.publishInputLevel(this.inputLevel.read());
    }, 80);
    this.unsubscribe = this.api.onEvent(event => this.receive(event));
    try {
      await this.api.start();
      if (generation !== this.generation) return;
      this.ready = true;
      this.capture = createMicrophoneCaptureLifecycle({ mediaDevices: navigator.mediaDevices,
        acquire: () => this.acquire(generation),
        onState: state => { if (state.state !== 'ready') this.resetInputLevel(); },
        onFatalError: error => {
          this.event({ type: 'error', message: error.message || '无法使用麦克风。' }); void this.stop();
        },
      });
      this.capture.start();
      this.follow = createPlaybackOutputFollow({ mediaDevices: navigator.mediaDevices, getContext: () => this.context });
      this.follow.start();
    } catch (error) { await this.stop(); throw error; }
  }
  private async acquire(generation: number) {
    const context = this.context!;
    const media = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
    const resampler = createStreamingResampler();
    let buffer = new Float32Array(0), pending = false, closed = false;
    const source = context.createMediaStreamSource(media);
    try {
      const workletURL = new URL('./microphone-audio-worklet-processor.js', import.meta.url).href;
      const processor = await createMicrophoneAudioWorkletNode({ context, moduleUrl: workletURL, onSamples: samples => {
        if (closed || !this.ready || this.muted || generation !== this.generation) { buffer = new Float32Array(0); resampler.reset(); return; }
        this.inputLevel.add(samples);
        const chunk = resampler.process(samples, context.sampleRate, 16000);
        const joined = new Float32Array(buffer.length + chunk.length); joined.set(buffer); joined.set(chunk, buffer.length); buffer = joined;
        if (buffer.length < 640) return;
        const audio = pcmBase64(buffer); buffer = new Float32Array(0);
        // Live capture must not accumulate stale audio while IPC is congested.
        if (pending) return;
        pending = true;
        void this.api.send({ type: 'input_audio_buffer.append', audio }).catch(() => false).finally(() => { pending = false; });
      } });
      source.connect(processor.node); processor.node.connect(context.destination);
      return { media, track: media.getAudioTracks()[0], close: () => { closed = true; this.resetInputLevel(); media.getTracks().forEach(track => track.stop()); source.disconnect(); processor.close(); } };
    } catch (error) { media.getTracks().forEach(track => track.stop()); source.disconnect(); throw error; }
  }
  private publishInputLevel(level: number) {
    if (level === this.lastInputLevel) return;
    this.lastInputLevel = level;
    this.event({ type: 'desktop.input-level', level });
  }
  private resetInputLevel() { this.inputLevel.reset(); this.publishInputLevel(0); }
  private receipt(type: string, responseId: string, reason?: string) {
    void this.api.send({ type, responseId, ...(reason ? { reason } : {}) }).catch(() => {});
  }
  private started(id: string, item: Playback) {
    if (this.playing.get(id) !== item || item.started) return;
    item.started = true; clearTimeout(item.timer); this.receipt('playback.started', id);
  }
  private finish(id: string, item: Playback) {
    if (this.playing.get(id) !== item || !item.done || item.sources.size) return;
    clearTimeout(item.timer); this.playing.delete(id); this.receipt('playback.ended', id);
    if (!this.playing.size) this.queue.reset();
    this.presentation.ended(id);
    this.presentation.tick(this.context?.currentTime || 0);
  }
  private play(event: VoiceEvent) {
    const context = this.context, id = String(event.responseId || '');
    if (!context || !id || typeof event.audio !== 'string' || this.suppressed.has(id)) return;
    const rate = Number(event.sampleRate || 24000);
    if (!Number.isFinite(rate) || rate < 8000 || rate > 96000) return;
    const samples = decodePcm(event.audio);
    if (!samples.length) return;
    let item = this.playing.get(id);
    if (!item) {
      item = { sources: new Set(), resampler: createStreamingResampler(), done: false, started: false };
      this.playing.set(id, item);
    }
    // Preserve phase across packets; per-source resampling introduces boundary spikes at 44.1 kHz.
    this.enqueue(id, item.resampler.process(samples, rate, context.sampleRate));
  }
  private enqueue(id: string, samples: Float32Array) {
    const context = this.context;
    if (!context || !samples.length) return;
    const bytes = samples.byteLength;
    // Bound PCM memory, not speech duration: TTS can arrive much faster than playback.
    if (this.queuedBytes + bytes > 64 * 1024 * 1024) {
      this.clear('playback_overflow');
      this.event({ type: 'error', message: '语音内容过长，已停止播报，请查看聊天中的完整结果。' });
      return;
    }
    this.queuedBytes += bytes;
    this.queue.push({ samples, sampleRate: context.sampleRate, responseId: id, duration: samples.length / context.sampleRate },
      { timelineAheadSeconds: this.nextTime - context.currentTime });
  }
  private schedule({ samples, sampleRate, responseId: id }: PcmPlaybackItem) {
    const context = this.context, playback = this.playing.get(id);
    if (!context || !playback) return;
    const bytes = samples.byteLength;
    const buffer = context.createBuffer(1, samples.length, sampleRate); buffer.copyToChannel(Float32Array.from(samples), 0);
    const source = context.createBufferSource(); source.buffer = buffer; source.connect(context.destination);
    const at = Math.max(this.nextTime, context.currentTime + audioSchedulingLeadSeconds());
    this.nextTime = at + buffer.duration;
    this.presentation.audio(id, at, buffer.duration);
    playback.sources.add(source);
    if (!playback.started && !playback.timer) playback.timer = setTimeout(() => this.started(id, playback), Math.max(0, (at - context.currentTime) * 1000));
    source.onended = () => {
      source.disconnect();
      if (this.playing.get(id) !== playback) return;
      this.queuedBytes -= bytes;
      this.started(id, playback); playback.sources.delete(source); this.finish(id, playback);
    };
    source.start(at);
  }
  private receive(event: VoiceEvent) {
    if (event.type === 'audio.done') {
      const id = String(event.responseId || ''), item = this.playing.get(id);
      if (item) {
        this.enqueue(id, item.resampler.flush());
        this.queue.finish();
      }
    }
    // Flush short replies before the presentation decides whether they contain audio.
    this.presentation.receive(event);
    if (event.type === 'playback.clear') this.clear(String(event.reason || 'interrupted'));
    if (event.type === 'audio.delta') this.play(event);
    if (event.type === 'audio.done') {
      const id = String(event.responseId || ''), item = this.playing.get(id);
      if (item) { item.done = true; this.finish(id, item); }
    }
    if (event.type === 'desktop.voice' && ['error', 'stopped'].includes(String(event.state))) { this.ready = false; this.capture?.stop(); this.resetInputLevel(); this.clear('disconnected'); }
    this.event(event);
    this.presentation.tick(this.context?.currentTime || 0);
  }
  clear(reason = 'user_interruption', preservePresentation = false) {
    if (!preservePresentation) this.presentation.clear();
    this.queue.reset();
    const items = [...this.playing]; this.playing.clear(); this.nextTime = 0; this.queuedBytes = 0;
    for (const [id, item] of items) {
      this.suppressed.add(id); clearTimeout(item.timer);
      for (const source of item.sources) { source.stop(); source.disconnect(); }
      this.receipt('playback.cancelled', id, reason);
    }
    while (this.suppressed.size > 256) this.suppressed.delete(this.suppressed.values().next().value!);
    this.presentation.tick(this.context?.currentTime || 0);
  }
  mute(value: boolean) { this.muted = value; this.resetInputLevel(); void this.api.send({ type: value ? 'input.mute' : 'input.unmute' }).catch(() => {}); }
  interrupt() { this.clear(); void this.api.send({ type: 'response.cancel' }).catch(() => {}); }
  async stop() {
    clearInterval(this.presentationTimer);
    this.resetInputLevel();
    this.generation++; this.ready = false; this.capture?.stop(); this.follow?.stop();
    // Preserve the rendered conversation and task records after hang-up.
    this.clear('call_ended', true);
    this.unsubscribe?.(); this.unsubscribe = undefined;
    const context = this.context; this.context = undefined;
    await Promise.all([context?.close().catch(() => {}), this.api.stop()]);
  }
}
