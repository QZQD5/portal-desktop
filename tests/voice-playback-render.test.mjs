// Render through Electron's real audio engine without speakers, microphone, or cloud services.
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { build } from 'esbuild';
import { launchDesktop } from './support/electron-lifecycle.mjs';

const root = path.resolve('test-results/voice-playback-render');
await mkdir(root, { recursive: true });
const profile = await mkdtemp(path.join(tmpdir(), 'voice-playback-render-'));
const bundle = await build({ entryPoints: ['desktop/renderer/voice/session.ts'], bundle: true, write: false,
  format: 'iife', globalName: 'VoicePlayback', logLevel: 'silent' });
await writeFile(path.join(root, 'main.cjs'), `const {app,BrowserWindow}=require('electron');
app.setPath('userData',process.env.PORTAL_DESKTOP_USER_DATA);
app.whenReady().then(()=>new BrowserWindow({show:false}).loadURL('about:blank'));`);
const app = await launchDesktop({ args: [path.join(root, 'main.cjs')], env: { ...process.env, PORTAL_DESKTOP_USER_DATA: profile } });
try {
  const page = await app.firstWindow();
  await page.addScriptTag({ content: bundle.outputFiles[0].text });
  const results = await page.evaluate(async () => {
    const inputRate = 24000, length = inputRate * 2;
    const pcm = new Uint8Array(length * 2), view = new DataView(pcm.buffer);
    for (let i = 0; i < length; i++) view.setInt16(i * 2, Math.round(Math.sin(i * 2 * Math.PI * 173 / inputRate + 0.8) * 16000), true);
    const results = [];
    for (const rate of [44100, 48000]) {
      async function render(chunkSize, jitter = false) {
        const context = new OfflineAudioContext(1, rate * 3, rate), starts = [];
        let now = 0;
        const session = new VoicePlayback.VoiceSession({ send: async () => true }, () => {});
        // Drive only the production playback path, replacing arrival time while retaining native audio nodes.
        session.context = {
          get currentTime() { return now; }, sampleRate: rate, destination: context.destination,
          createBuffer: context.createBuffer.bind(context),
          createBufferSource() {
            const source = context.createBufferSource(), start = source.start.bind(source);
            source.start = at => { starts.push({ at, duration: source.buffer.duration }); start(at); };
            return source;
          },
        };
        for (let offset = 0, i = 0; offset < length; offset += chunkSize, i++) {
          if (jitter) now = offset / inputRate + (i % 5 === 2 ? 0.07 : i % 5 === 3 ? 0.04 : 0);
          let binary = '';
          for (const byte of pcm.subarray(offset * 2, (offset + chunkSize) * 2)) binary += String.fromCharCode(byte);
          session.receive({ type: 'audio.delta', responseId: 'tone', sampleRate: inputRate, audio: btoa(binary) });
        }
        session.receive({ type: 'audio.done', responseId: 'tone' });
        const output = (await context.startRendering()).getChannelData(0);
        session.clear('test_complete');
        const begin = Math.round(starts[0].at * rate);
        return { samples: output.slice(begin, begin + rate * 2), starts };
      }
      const reference = await render(length);
      for (const size of [960, 997]) for (const jitter of [false, true]) {
        const actual = await render(size, jitter);
        let maxError = 0, maxGap = 0;
        for (let i = 25; i < reference.samples.length - 25; i++) maxError = Math.max(maxError, Math.abs(actual.samples[i] - reference.samples[i]));
        for (let i = 1; i < actual.starts.length; i++) {
          const previous = actual.starts[i - 1];
          maxGap = Math.max(maxGap, Math.abs(actual.starts[i].at - previous.at - previous.duration));
        }
        results.push({ rate, size, jitter, maxError, maxGap, duration: actual.starts.reduce((sum, source) => sum + source.duration, 0) });
      }
    }
    return results;
  });
  await writeFile(path.join(root, 'metrics.json'), JSON.stringify(results, null, 2));
  for (const result of results) {
    assert.ok(result.maxGap < 1e-9, `Packet gap: ${JSON.stringify(result)}`);
    assert.ok(result.maxError < 0.002, `Packet boundary spike: ${JSON.stringify(result)}`);
    assert.ok(Math.abs(result.duration - 2) < 1e-9, `Lost or repeated PCM: ${JSON.stringify(result)}`);
  }
  console.log('PASS: 44.1/48 kHz native rendering, irregular packet sizes, 70 ms jitter, complete PCM duration.');
  console.log(JSON.stringify(results));
} finally { await app.close(); }
