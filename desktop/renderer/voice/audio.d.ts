export function createStreamingResampler(): { process(input: Float32Array, from: number, to: number): Float32Array; reset(): void; flush(): Float32Array };
export function pcmBase64(samples: Float32Array): string;
export function decodePcm(base64: string): Float32Array;
export function audioSchedulingLeadSeconds(): number;
export interface PcmPlaybackItem { samples: Float32Array; sampleRate: number; responseId: string; duration: number }
export function createPcmPlaybackQueue(options: { remote?: boolean; onFlush(items: PcmPlaybackItem[]): void }): {
  push(item: PcmPlaybackItem, options?: { timelineAheadSeconds?: number }): void;
  finish(): void; responseIds(): string[]; reset(): void;
};
