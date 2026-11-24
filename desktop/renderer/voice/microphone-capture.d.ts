export function createMicrophoneCaptureLifecycle(options: {
  mediaDevices: MediaDevices; acquire(): Promise<{ media: MediaStream; track: MediaStreamTrack; close(): void }>;
  onFatalError?(error: Error): void; onState?(state: { state: NonNullable<import('../../shared/voice').VoiceState['microphone']> }): void;
}): { start(): void; stop(): void; restart(reason?: string): void };
