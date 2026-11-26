import { defineConfig } from 'vite';
import path from 'node:path';
import react from '@vitejs/plugin-react';
export default defineConfig({
  plugins: [react()],
  optimizeDeps: { include: ['onnxruntime-web/wasm'] },
  root: 'desktop/renderer', publicDir: '../generated', base: './',
  build: { outDir: path.resolve('.vite/renderer/main_window'),
    assetsInlineLimit: file => file.endsWith('microphone-audio-worklet-processor.js') ? false : undefined },
});
