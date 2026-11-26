import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";
import { createProxy } from "./web/proxy.mjs";
import { installVoiceProxy } from "./web/voice-proxy.mjs";
import { voiceInstructions } from "./scripts/voice-instructions.ts";

export default defineConfig({
  root: "web",
  base: './',
  define: { PORTAL_VOICE_INSTRUCTIONS: JSON.stringify(voiceInstructions) },
  worker: { format: "es" },
  optimizeDeps: { include: ["onnxruntime-web/wasm"] },
  plugins: [
    react(),
    {
      name: "town-web-api",
      configureServer(server) {
        server.middlewares.use(createProxy());
        if (server.httpServer) installVoiceProxy(server.httpServer);
      },
    },
  ],
  server: { host: "127.0.0.1", port: 5174 },
  build: {
    target: "es2022",
    outDir: path.resolve("out/web/public"),
    emptyOutDir: true,
    rollupOptions: {
      input: {
        index: path.resolve("web/index.html"),
        chat: path.resolve("web/chat.html"),
      },
    },
  },
});
