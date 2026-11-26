import { env, InferenceSession, Tensor } from 'onnxruntime-web/wasm';
import { decodePcm } from './audio.js';

// Silero v6.2.3, 16 kHz opset 15, from snakers4/silero-vad commit
// 5cd7945676eb32225748052e2e6a0580e4686a08 (MIT). Each call owns its recurrent state.
env.wasm.numThreads = 1;
env.wasm.wasmPaths = {
  mjs: new URL('../../../node_modules/onnxruntime-web/dist/ort-wasm-simd-threaded.mjs', import.meta.url).href,
  wasm: new URL('../../../node_modules/onnxruntime-web/dist/ort-wasm-simd-threaded.wasm', import.meta.url).href,
};
const model = InferenceSession.create(new URL('./silero-v6.2.3.onnx', import.meta.url).href, {
  executionProviders: ['wasm'],
});
const sampleRate = new Tensor('int64', BigInt64Array.of(16000n), []);
const input = new Float32Array(64 + 512);
const frame = new Tensor('float32', input, [1, input.length]);
let recurrent = new Tensor('float32', new Float32Array(256), [2, 1, 128]);
let used = 64,
  voiced = 0,
  quietFrames = 0,
  speaking = false;

async function analyze(audio: string, reset: boolean) {
  const session = await model;
  if (reset) {
    input.fill(0);
    recurrent.data.fill(0);
    used = 64;
    voiced = 0;
    quietFrames = 0;
    speaking = false;
  }
  const samples = decodePcm(audio);
  let started = false;
  for (let pos = 0; pos < samples.length;) {
    const count = Math.min(input.length - used, samples.length - pos);
    input.set(samples.subarray(pos, pos + count), used);
    pos += count;
    used += count;
    if (used !== input.length) continue;
    const { output, stateN } = await session.run({ input: frame, state: recurrent, sr: sampleRate });
    recurrent = stateN as typeof recurrent;
    input.copyWithin(0, 512);
    used = 64;
    const probability = Number(output.data[0]);
    if (probability >= 0.6) {
      quietFrames = 0;
      voiced++;
      if (!speaking && voiced >= 3) {
        speaking = true;
        started = true;
      }
    } else if (probability < 0.45) {
      voiced = 0;
      quietFrames++;
      if (quietFrames >= 10) speaking = false;
      if (quietFrames % 16 === 0) {
        input.fill(0, 0, 64);
        recurrent.data.fill(0);
      }
    } else {
      voiced = 0;
      quietFrames = 0;
    }
  }
  return { started, speaking, barge: speaking && voiced >= 28, quietFrames };
}

let queue = Promise.resolve();
self.onmessage = ({ data }: MessageEvent<{ audio: string; reset: boolean }>) => {
  queue = queue
    .then(async () => self.postMessage(await analyze(data.audio, data.reset)))
    .catch(() => self.postMessage({ error: '本地人声检测加载或运行失败。' }));
};
void model.catch(() => {}); // Initialization errors are returned to the first request.
