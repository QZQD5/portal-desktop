export function createPlaybackOutputFollow(options: { mediaDevices: MediaDevices; getContext(): AudioContext | undefined }): { start(): void; stop(): void };
