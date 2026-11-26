export interface VoiceActivity {
  started: boolean;
  speaking: boolean;
  barge: boolean;
  quietFrames: number;
}

/** Audio capture submits in order. On stop, release both the model and any pending sends. */
export class ActivityDetector {
  private worker = new Worker(new URL('./activity-worker.ts', import.meta.url), { type: 'module' });
  private pending: { resolve(value: VoiceActivity): void; reject(reason: Error): void }[] = [];
  private failure?: Error;

  constructor() {
    this.worker.onmessage = ({ data }: MessageEvent<VoiceActivity | { error: string }>) => {
      if ('error' in data) this.close(new Error(data.error));
      else this.pending.shift()?.resolve(data);
    };
    this.worker.onerror = () => this.close(new Error('本地人声检测未能启动。'));
  }
  analyze(audio: string, reset: boolean): Promise<VoiceActivity> {
    if (this.failure) return Promise.reject(this.failure);
    return new Promise((resolve, reject) => {
      this.pending.push({ resolve, reject });
      this.worker.postMessage({ audio, reset });
    });
  }
  close(reason = new Error('语音通话已结束。')) {
    this.failure = reason;
    this.worker.terminate();
    for (const request of this.pending.splice(0)) request.reject(reason);
  }
}
