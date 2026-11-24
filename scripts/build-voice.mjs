import { spawn } from 'node:child_process';
import { cp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

const argument = name => { const index = process.argv.indexOf(name); return index < 0 ? undefined : process.argv[index + 1]; };
const source = argument('--source') || process.env.PORTAL_DESKTOP_GATEWAY_ROOT || 'audio-agent-gateway';
if (!['win32', 'darwin'].includes(process.platform)) throw new Error('The bundled Gateway supports Windows and macOS.');
const python = argument('--python') || process.env.PORTAL_DESKTOP_GATEWAY_PYTHON || 'python';
const root = path.resolve(source);
await new Promise((resolve, reject) => {
  const identity = argument('--codesign-identity');
  const child = spawn(python, [path.join(root, 'scripts/build_windows.py'), ...(identity ? ['--codesign-identity', identity] : [])], { cwd: root, stdio: 'inherit', shell: false, windowsHide: true });
  child.once('error', reject); child.once('exit', code => code === 0 ? resolve() : reject(new Error(`Gateway build failed (${code})`)));
});
const destination = path.resolve('resources/audio-gateway');
// This generated directory belongs to this build; don't retain obsolete runtime modules.
await rm(destination, { recursive: true, force: true });
await mkdir(destination, { recursive: true });
await cp(path.join(root, 'scripts/dist/AudioAgentGateway'), destination, { recursive: true, verbatimSymlinks: true });
const executable = process.platform === 'win32' ? 'AudioAgentGateway.exe' : 'AudioAgentGateway';
const sha256 = createHash('sha256').update(await readFile(path.join(destination, executable))).digest('hex');
await writeFile(path.join(destination, 'runtime.json'), JSON.stringify({ schema: 1, protocol: '7.0.0', platform: process.platform, arch: process.arch, sha256 }, null, 2));
console.log(`Prepared ${process.platform}/${process.arch} voice runtime in resources/audio-gateway`);
