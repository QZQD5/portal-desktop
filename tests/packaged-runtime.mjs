import assert from 'node:assert/strict';
import { cp, mkdir, mkdtemp, readFile, realpath, rm, stat, writeFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { desktopExecutable } from './support/desktop.mjs';
import { launchDesktop } from './support/electron-lifecycle.mjs';

const executable = await desktopExecutable();
const source = process.platform === 'darwin' ? path.resolve(path.dirname(executable), '../..') : path.dirname(executable);
const directory = await realpath(await mkdtemp(path.join(os.tmpdir(), 'town installed runtime ')));
let app;
try {
  // Relocate the full app to a path with spaces, away from checkout/node_modules.
  const destination = path.join(directory, path.basename(source));
  await cp(source, destination, { recursive: true, verbatimSymlinks: true });
  const installed = path.join(destination, path.relative(source, executable));
  const resources = process.platform === 'darwin' ? path.join(destination, 'Contents/Resources') : path.join(destination, 'resources');
  const fixture = path.join(directory, 'runtime.cjs');
  await cp(fileURLToPath(new URL('./fixtures/packaged-runtime.cjs', import.meta.url)), fixture);
  const environment = Object.fromEntries(Object.entries(process.env).filter(([key]) => !['PATH', 'NODE_PATH', 'NODE_OPTIONS'].includes(key.toUpperCase())));
  const runOptions = { cwd: directory, stdio: 'inherit', timeout: 90_000,
    env: { ...environment, ELECTRON_RUN_AS_NODE: '1', PATH: process.platform === 'win32'
      ? path.join(process.env.SystemRoot, 'System32') : '/usr/bin:/bin' } };
  execFileSync(installed, [fixture, resources], runOptions);
  assert.equal(await stat(path.join(resources, 'audio-gateway')).catch(error => {
    if (error.code === 'ENOENT') return null;
    throw error;
  }), null);
  if (process.platform === 'darwin') {
    const info = path.join(destination, 'Contents/Info.plist');
    assert.ok(execFileSync('/usr/libexec/PlistBuddy', ['-c', 'Print :NSMicrophoneUsageDescription', info], {encoding:'utf8'}).trim());
    const entitlements = execFileSync('/usr/bin/codesign', ['--display','--entitlements',':-',destination], {encoding:'utf8'});
    assert.match(entitlements, /<key>com\.apple\.security\.device\.audio-input<\/key>\s*<true\s*\/>/);
  }

  // Reopen an existing profile with the old settings/plugin-record formats.
  const profile = path.join(directory, 'profile'), workspace = path.join(directory, '工作区');
  await mkdir(workspace);
  const launch = { executablePath: installed, cwd: directory,
    env: { ...environment, PORTAL_DESKTOP_USER_DATA: profile, PORTAL_DESKTOP_TEST_MOCK_KEYCHAIN: '1' } };
  app = await launchDesktop(launch);
  let page = await app.firstWindow();
  await page.waitForFunction(() => Boolean(window.beings));
  const initial = await page.evaluate(() => window.beings.snapshot());
  const credential = await app.evaluate(({ safeStorage }) => safeStorage.encryptString(JSON.stringify({ link: 'http://127.0.0.1:1/retained/?token=fixture' })).toString('base64'));
  assert.equal(await app.evaluate(({ app }) => app.isPackaged), true);
  // Seed a pre-Gateway IndexedDB in the same partition and top-level site as the chat iframe.
  await app.evaluate(async ({BrowserWindow}) => {
    const seed = new BrowserWindow({show:false});
    try {
      await seed.loadURL('beings://desktop/client-context-host.html');
      const frame = seed.webContents.mainFrame.frames.find(frame => frame.url === 'beings://chat/client-context.html');
      const seedRows = () => new Promise((resolve,reject) => {
        const opening = indexedDB.open('loom-history-' + encodeURIComponent('http://127.0.0.1:1/retained'),2);
        opening.onupgradeneeded = () => {
          const messages = opening.result.createObjectStore('messages',{keyPath:'seq'});
          messages.createIndex('scene','scene_id');
          messages.put({seq:81,role:'user',content:'升级前的本地消息',scene_id:'old-room'});
          opening.result.createObjectStore('meta',{keyPath:'key'});
        };
        opening.onsuccess = () => { opening.result.close(); resolve(); };
        opening.onerror = () => reject(opening.error);
      });
      await frame.executeJavaScript('(' + seedRows.toString() + ')()');
    } finally { seed.destroy(); }
  });
  await app.close(); app = undefined;
  const settings = { ...initial.settings, workspace, portalBinary: path.join(directory, 'removed-old-app/heart-portal'),
    portalName: 'retained-profile', autoStart: false, backgroundEnabled: false, allowExec: false, kitsEnabled: false };
  const saved = JSON.stringify({ version: 1, settings, credential });
  await writeFile(path.join(profile, 'connection.json'), saved);
  const plugins = path.join(profile, 'third-party-plugins'), root = path.join(plugins, 'packages/retained-plugin');
  await mkdir(root, { recursive: true });
  await mkdir(path.join(plugins, 'data'), { recursive: true });
  await writeFile(path.join(root, 'package.json'), JSON.stringify({ name: 'retained-plugin', version: '1.0.0', main: 'index.cjs', dsh: {} }));
  await writeFile(path.join(root, 'index.cjs'), 'exports.apply = () => {};');
  const records = JSON.stringify([{ root, plugin: { id: 'retained-plugin', version: '1.0.0', source: root,
    enabled: true, installedAt: '2026-01-01T00:00:00.000Z', status: 'active' }, config: { retained: '原配置' } }]);
  await writeFile(path.join(plugins, 'plugins.json'), records);
  await writeFile(path.join(plugins, 'data/retained.txt'), '插件原数据');
  app = await launchDesktop(launch);
  page = await app.firstWindow();
  await page.waitForFunction(() => Boolean(window.beings));
  const restored = await page.evaluate(() => window.beings.snapshot());
  const workerUrl = await app.evaluate(() => {
    const fs = process.getBuiltinModule('node:fs'), path = process.getBuiltinModule('node:path');
    const root = path.join(process.resourcesPath, 'app.asar/.vite/renderer/main_window');
    const worker = fs.readdirSync(root, { recursive:true }).find(file => /^activity-worker(?:-[\w-]+)?\.js$/.test(path.basename(file)));
    if (!worker) throw new Error('Packaged Silero worker missing');
    return 'beings://desktop/' + worker.split(path.sep).join('/');
  });
  const vad = await page.evaluate(workerUrl => new Promise((resolve, reject) => {
    const worker = new Worker(workerUrl, { type:'module' });
    const timer = setTimeout(() => { worker.terminate(); reject(new Error('Silero inference timed out')); }, 30000);
    worker.onmessage = ({data}) => { clearTimeout(timer); worker.terminate(); resolve(data); };
    worker.onerror = event => { clearTimeout(timer); worker.terminate(); reject(new Error(event.message)); };
    worker.postMessage({ audio:btoa('\0'.repeat(512 * 2)), reset:true });
  }), workerUrl);
  assert.deepEqual(vad, {started:false, speaking:false, barge:false, quietFrames:1});
  await app.evaluate(async ({BrowserWindow}) => {
    const client = new BrowserWindow({show:false});
    try {
      await client.loadURL('beings://desktop/client-context-host.html');
      const frame = client.webContents.mainFrame.frames.find(frame => frame.url === 'beings://chat/client-context.html');
      await frame.executeJavaScript('window.clientCommand("http://127.0.0.1:1/retained","scenes","")');
    } finally { client.destroy(); }
  });
  const history = await app.evaluate(async ({net}) => {
    const response = await net.fetch('beings://chat/api/local-history?limit=1');
    return {status:response.status, body:await response.json()};
  });
  assert.equal(history.status, 200);
  assert.equal(history.body.messages[0].content,'升级前的本地消息');
  assert.equal(history.body.messages[0].upstream_seq,81);
  console.log('PASS relocated voice runtime: Silero inferred a PCM frame and native SQLite imported the original IndexedDB message.');
  assert.equal(restored.settings.workspace, workspace);
  assert.equal(restored.settings.portalName, 'retained-profile');
  assert.equal(restored.settings.hasToken, true);
  assert.equal(restored.settings.allowExec, false);
  assert.equal(restored.settings.portalBinary, path.join(resources, process.platform === 'win32' ? 'heart-portal.exe' : 'heart-portal'));
  await page.waitForFunction(async () => (await window.beings.dshPlugins.state()).plugins[0]?.status === 'active');
  const state = await page.evaluate(() => window.beings.dshPlugins.state());
  assert.deepEqual(state.plugins[0].config, { retained: '原配置' });
  assert.equal(await readFile(path.join(profile, 'connection.json'), 'utf8'), saved);
  assert.equal(await readFile(path.join(plugins, 'plugins.json'), 'utf8'), records);
  assert.equal(await readFile(path.join(plugins, 'data/retained.txt'), 'utf8'), '插件原数据');
  console.log('PASS packaged profile reopen: current bundled Portal path, retained connection/workspace/settings/plugin records/data, existing plugin active.');
} finally {
  if (app) await app.close();
  // Windows can keep the relocated executable locked briefly after Electron exits.
  await rm(directory, { recursive: true, force: true, maxRetries: 10, retryDelay: 250 });
}
