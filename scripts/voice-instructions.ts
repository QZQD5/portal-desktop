import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

// Vite embeds the same source prompts as the reference gateway in both native clients.
const read = (file: string) =>
  readFileSync(resolve('audio-agent-gateway/gateway/resources', file), 'utf8').trim();
export const voiceInstructions = [
  ...['PROMPT.md', 'ROUTING-PROMPT.md', 'BEING-PROMPT.md', 'MEMORY-PROMPT.md'].map(read),
  '<assistant_profile>' + read('ASSISTANT.md').slice(0, 4000) + '</assistant_profile>',
].join('\n');
