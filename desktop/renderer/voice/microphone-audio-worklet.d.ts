export function createMicrophoneAudioWorkletNode(options: { context: AudioContext; moduleUrl: string; onSamples(samples: Float32Array): void }): Promise<{ node: AudioWorkletNode; close(): void }>;
