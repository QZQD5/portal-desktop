import { build } from "vite";
import { build as bundle } from "esbuild";
import { copyFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { viteModules, writeThirdPartyLicenses } from "./third-party-licenses.mjs";

const result = await build({ configFile: path.resolve("vite.web.config.mjs") });
// Include ws in the standalone server so deployments still need only Node.
const serverBundle = await bundle({
  entryPoints: ["web/voice-proxy.mjs"], outfile: "out/web/voice-proxy.mjs", bundle: true,
  platform: "node", format: "esm", metafile: true, external: ["bufferutil", "utf-8-validate"],
  banner: { js: 'import { createRequire } from "node:module"; const require = createRequire(import.meta.url);' },
});
for (const file of ["server.mjs", "proxy.mjs", "README.md", "OSS.md"])
  await copyFile(`web/${file}`, `out/web/${file}`);
await copyFile("LICENSE", "out/web/LICENSE");
await copyFile(
  "desktop/renderer/shared/components/react-bits-stack/LICENSE.md",
  "out/web/React-Bits-LICENSE.md",
);
await copyFile(
  "desktop/renderer/shared/components/animate-ui.LICENSE.md",
  "out/web/Animate-UI-LICENSE.md",
);
await writeThirdPartyLicenses("out/web/THIRD-PARTY-LICENSES.txt", [...viteModules(result), ...Object.keys(serverBundle.metafile.inputs)]);
await writeFile(
  "out/web/package.json",
  JSON.stringify(
    {
      name: "beings-town-web",
      private: true,
      type: "module",
      scripts: { start: "node server.mjs" },
      engines: { node: ">=22.12" },
    },
    null,
    2,
  ) + "\n",
);
console.log(
  "Standalone web build: out/web (no Electron, Rust or npm install needed to serve).",
);
