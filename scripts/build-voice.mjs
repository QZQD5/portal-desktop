import { spawn } from 'node:child_process';
import { cp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

const argument = name => { const index = process.argv.indexOf(name); return index < 0 ? undefined : process.argv[index + 1]; };
const source = argument('--source') || process.env.PORTAL_DESKTOP_GATEWAY_ROOT || 'audio-agent-gateway';
if (process.platform !== 'win32') throw new Error('The voice milestone currently builds Windows only.');
const python = argument('--python') || process.env.PORTAL_DESKTOP_GATEWAY_PYTHON || 'python';
const root = path.resolve(source);
await new Promise((resolve, reject) => {
  const child = spawn(python, [path.join(root, 'scripts/build_windows.py')], { cwd: root, stdio: 'inherit', shell: false, windowsHide: true });
  child.once('error', reject); child.once('exit', code => code === 0 ? resolve() : reject(new Error(`Gateway build failed (${code})`)));
});
const destination = path.resolve('resources/audio-gateway');
// This generated directory belongs to this build; don't retain obsolete runtime modules.
await rm(destination, { recursive: true, force: true });
await mkdir(destination, { recursive: true });
await cp(path.join(root, 'scripts/dist/AudioAgentGateway'), destination, { recursive: true });
const sha256 = createHash('sha256').update(await readFile(path.join(destination, 'AudioAgentGateway.exe'))).digest('hex');
await writeFile(path.join(destination, 'runtime.json'), JSON.stringify({ schema: 1, protocol: '7.0.0', platform: process.platform, arch: process.arch, sha256 }, null, 2));
console.log('Prepared Windows voice runtime in resources/audio-gateway');
