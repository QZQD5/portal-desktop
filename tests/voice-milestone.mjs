// Real Electron capture, local Silero and playback, against loopback Being and voice fixtures.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { builtinModules } from 'node:module';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { once } from 'node:events';
import path from 'node:path';
import { build } from 'esbuild';
import { createServer as viteServer, build as viteBuild } from 'vite';
import { WebSocketServer } from 'ws';
import { launchDesktop } from './support/electron-lifecycle.mjs';

const root = path.resolve('test-results/voice-milestone');
await mkdir(root, { recursive: true });
const profile = await mkdtemp(path.join(tmpdir(), 'portal-voice-'));
const external = process.argv.includes('--external');
let manual;
async function startManual(port = 0) {
  const gatewayRoot = process.env.PORTAL_DESKTOP_GATEWAY_ROOT || path.resolve('audio-agent-gateway');
  const python = process.env.PORTAL_DESKTOP_GATEWAY_PYTHON || path.join(gatewayRoot, '.venv',
    process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
  const child = spawn(python,
    ['-u', '-m', 'gateway', '--dev', '--port', String(port), '--data-dir', path.join(profile, 'gateway')],
    { cwd: gatewayRoot, windowsHide: true, stdio: 'pipe',
      env: { ...process.env, PYTHONUTF8: '1', DOUBAO_API_KEY: 'voice-test-key' } });
  manual = child;
  child.stderr.resume();
  const lines = createInterface({ input: child.stdout });
  try {
    return await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Manual Gateway startup timed out')), 60000);
      const fail = error => { clearTimeout(timer); reject(error); };
      child.once('error', fail);
      child.once('exit', code => fail(new Error('Manual Gateway exited: ' + code)));
      lines.on('line', line => {
        try {
          const event = JSON.parse(line);
          if (event.type === 'gateway.ready') { clearTimeout(timer); resolve(event.origin); }
        } catch { /* Non-control output. */ }
      });
    });
  } finally { lines.close(); }
}
async function stopManual() {
  if (manual && manual.exitCode === null && manual.signalCode === null) {
    const exited = once(manual, 'exit'); manual.kill(); await exited;
  }
}
const requests = [], speech = [], cancelledResponses = [], stoppedTasks = [];
let mode = 'hello', needTurn = false, audioInputs = 0, heldResponse = '', lateResponseId = '', lastInputReplyId = '';
const server = createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  assert.equal(url.searchParams.get('token'), 'heart-test-key');
  if (url.pathname.endsWith('/api/status')) { response.setHeader('Content-Type', 'application/json'); response.end('{"being_name":"Fixture"}'); return; }
  if (url.pathname.endsWith('/api/stream/active')) { response.writeHead(204); response.end(); return; }
  if (url.pathname.endsWith('/api/stop')) { stoppedTasks.push(url.pathname); response.writeHead(200).end('{"ok":true}'); return; }
  if (url.pathname.endsWith('/api/chat/stream')) {
    let body = ''; for await (const chunk of request) body += chunk;
    requests.push(JSON.parse(body));
    response.writeHead(200, { 'Content-Type': 'text/event-stream' });
    for (const [event, data] of [['meta', { stream_id: 'fixture-task' }], ['tool_use', { name: 'fixture' }],
      ['content_block_delta', { delta: { text: '任务已经由 Being 完成。' } }], ['message_stop', {}]]) response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    response.end(); return;
  }
  response.writeHead(404); response.end();
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const wss = new WebSocketServer({ host: '127.0.0.1', port: 0 });
await new Promise(resolve => wss.once('listening', resolve));
wss.on('connection', ws => {
  let current = '', waiting = false;
  const send = value => { if (ws.readyState === 1) ws.send(JSON.stringify(value)); };
  const finish = text => {
    speech.push(text);
    send({ type: 'response.output_text.delta', response_id: current, text });
    send({ type: 'response.output_text.done', response_id: current, text });
    // Deliver a long Being result faster than playback, as real TTS does.
    const long = text === '任务已经由 Being 完成。';
    for (let i = 0; i < (long ? 40 : 1); i++)
      send({ type: 'response.output_audio.delta', response_id: current, audio: Buffer.alloc(long ? 48000 : 9600).toString('base64') });
    send({ type: 'response.output_audio.done', response_id: current });
    send({ type: 'response.done', response: { usage: { total_tokens: 1 } } }); current = '';
  };
  ws.on('message', raw => {
    const event = JSON.parse(raw);
    if (event.type === 'session.create' || event.type === 'session.update') send({ type: event.type === 'session.create' ? 'session.created' : 'session.updated' });
    if (event.type === 'input_audio_buffer.append') {
      audioInputs++;
      if (needTurn) {
        needTurn = false; const text = mode, item = crypto.randomUUID();
        send({ type: 'conversation.item.input_audio_transcription.started', item_id: item });
        setTimeout(() => {
          send({ type: 'conversation.item.input_audio_transcription.completed', item_id: item, text });
          if (lateResponseId) {
            const tail = { type: 'response.output_audio.delta', audio: Buffer.alloc(9600).toString('base64') };
            send({ ...tail, response_id: lateResponseId });
            send(tail);
            lateResponseId = '';
          }
          current = crypto.randomUUID(); lastInputReplyId = current;
          send({ type: 'response.output_audio.started', response_id: current });
          if (text === 'delegate') {
            waiting = true;
            send({ type: 'response.function_call_arguments.done', response_id: current, items: [{ name: 'spawn_thinking', call_id: crypto.randomUUID(), arguments: JSON.stringify({ objective: '执行里程碑测试任务' }) }] });
          } else if (text === 'hold') {
            heldResponse = current;
            send({ type: 'response.output_text.delta', response_id: current, text: '正在播报长回复' });
            send({ type: 'response.output_audio.delta', response_id: current, audio: Buffer.alloc(24000 * 2 * 10).toString('base64') });
          } else finish(text === 'hello' ? '你好，实时语音已经连接。' : '收到你的问题：' + text + '。');
        }, 80);
      }
    }
    if (event.type === 'conversation.item.create' && waiting && event.items?.some(item => item.role === 'tool')) { waiting = false; finish('已经交给 Being。'); }
    if (event.type === 'speech_text_buffer.commit') { current = crypto.randomUUID(); send({ type: 'response.output_audio.started', response_id: current }); finish(event.text); }
    if (event.type === 'response.cancel' && current) {
      cancelledResponses.push(current);
      send({ type: 'response.canceled', response_id: current });
      send({ type: 'response.output_audio.delta', response_id: current, audio: Buffer.alloc(9600).toString('base64') });
      current = ''; waiting = false;
    }
  });
});
// Exercise production bundling, including ws's optional-native-module handling.
await viteBuild({ configFile: 'vite.main.config.ts',
  resolve: { conditions: ['node'], mainFields: ['module', 'jsnext:main', 'jsnext'] },
  build: { outDir: root, emptyOutDir: false, copyPublicDir: false,
    rollupOptions: { external: builtinModules.flatMap(name => [name, 'node:' + name]) },
    lib: { entry: 'desktop/main/voice/gateway.ts', formats: ['cjs'], fileName: () => 'gateway.cjs' } } });
await build({ entryPoints: ['desktop/main/app/protocol.ts'], outfile: path.join(root, 'protocol.cjs'), bundle: true, platform: 'node', format: 'cjs', external: ['electron'] });
await build({ entryPoints: ['desktop/main/chat/proxy.ts'], outfile: path.join(root, 'proxy.cjs'), bundle: true, platform: 'node', format: 'cjs' });
await build({ stdin: { contents: `import './desktop/preload/preload';import {contextBridge,ipcRenderer} from 'electron';contextBridge.exposeInMainWorld('fixture',{sendText:()=>ipcRenderer.invoke('fixture:text')});`, resolveDir: process.cwd() },
  outfile: path.join(root, 'text-preload.cjs'), bundle: true, platform: 'node', format: 'cjs', external: ['electron'] });
await writeFile(path.join(root, 'index.html'), `<!doctype html><html lang="zh-CN"><meta charset="UTF-8"><div id="root"></div><script type="module">
import React from 'react';
import {createRoot} from 'react-dom/client';
import {useVoice} from '/desktop/renderer/voice/use-voice.ts';
import {VoiceActivity,VoiceControl} from '/desktop/renderer/voice/control.tsx';
import '/desktop/renderer/voice/_control.scss';
function Fixture(){const voice=useVoice(window.beings.voice,'fixture',true);window.voiceState=voice.state;return React.createElement('div',{id:'input-area',style:{position:'absolute',bottom:30,left:60,right:60}},React.createElement('button',{onClick:()=>window.fixture.sendText().then(text=>{window.textResult=text})},'发送文字打断'),React.createElement(VoiceActivity,{state:voice.state,command:voice.command}),React.createElement('div',{id:'input-row',style:{display:'grid',position:'relative'}},React.createElement(VoiceControl,{state:voice.state,command:voice.command})));}
createRoot(document.getElementById('root')).render(React.createElement(Fixture));
</script></html>`);
const vite = await viteServer({ configFile: false, root: process.cwd(), server: { host: '127.0.0.1', port: 0, hmr:false, watch:null },
  optimizeDeps: { entries: [path.join(root, 'index.html')], include: ['react', 'react-dom/client', 'react/jsx-runtime', 'onnxruntime-web/wasm'] }, esbuild: { jsx: 'automatic' } });
await vite.listen();
const url = vite.resolvedUrls.local[0] + 'test-results/voice-milestone/index.html';
await writeFile(path.join(root, 'main.cjs'), `const {app,BrowserWindow,ipcMain}=require('electron');
app.setPath('userData',process.env.PORTAL_DESKTOP_USER_DATA);
const {VoiceGateway}=require('./gateway.cjs'); const {configureLocalSession}=require('./protocol.cjs'); const {ChatProxy}=require('./proxy.cjs');
app.whenReady().then(async()=>{
 const w=new BrowserWindow({show:false,width:900,height:650,webPreferences:{preload:require('node:path').join(__dirname,'text-preload.cjs'),contextIsolation:true,sandbox:true,additionalArguments:${JSON.stringify(external ? ['--portal-external-voice-gateway'] : [])}}});
 const voice=new VoiceGateway({directory:app.getPath('userData'),resources:${JSON.stringify(path.resolve('resources'))},packaged:false,
 binding:()=>({connection:{endpoint:'http://127.0.0.1:${server.address().port}/fixture',token:'heart-test-key'},scene:{scene_id:'fixture-scene',scene_meta:{client:'desktop',scene_label:'Fixture'}}}),emit:e=>{if(!w.isDestroyed()) w.webContents.send('beings:voice-event',e)}});
 const chat=new ChatProxy(()=>({endpoint:'http://127.0.0.1:${server.address().port}/fixture',token:'heart-test-key'}),(url,init)=>voice.fetchChat(url,init),()=>({scene_id:'fixture-scene',scene_meta:{client:'desktop',scene_label:'Fixture'}}));
 ipcMain.handle('fixture:text',async()=>{const r=await chat.handle(new Request('beings://chat/api/chat/stream',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:'文字打断',client_request_id:'interrupt-once'})}));if(!r.ok)throw new Error('Text send failed');return r.text()});
 configureLocalSession(()=>${JSON.stringify(url)});
 for(const [name,fn] of Object.entries({'start':()=>voice.start(),'stop':()=>voice.disconnect(),'send':e=>voice.send(e)})) ipcMain.handle('beings:voice-'+name,(_e,...args)=>fn(...args));
 let quitting=false; app.on('before-quit',e=>{if(!quitting){e.preventDefault();quitting=true;voice.shutdown().finally(()=>app.quit());}});
 await w.loadURL(${JSON.stringify(url)});
});`);
let app;
try {
  const origin = external ? await startManual() : '';
  app = await launchDesktop({ ...(process.env.PORTAL_DESKTOP_ELECTRON_EXECUTABLE ? { executablePath: process.env.PORTAL_DESKTOP_ELECTRON_EXECUTABLE } : {}), args: [path.join(root, 'main.cjs'), '--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream'],
    env: { ...process.env, PORTAL_DESKTOP_USER_DATA: profile, PORTAL_DESKTOP_VOICE_PROVIDER_URL: `ws://127.0.0.1:${wss.address().port}`,
      PORTAL_DESKTOP_GATEWAY_URL: origin, DOUBAO_API_KEY: external ? '' : 'voice-test-key' } });
  const page = await app.firstWindow();
  const speechFixture = await readFile('audio-agent-gateway/tests/fixtures/speech-stop.pcm');
  const probe = Buffer.concat([Buffer.alloc(32768), speechFixture, Buffer.alloc(32768)]);
  const frames = await page.evaluate(async audio => {
    const { ActivityDetector } = await import('/desktop/renderer/voice/activity.ts');
    const detector = new ActivityDetector(), pcm = atob(audio), frames = [];
    try {
      for (let i = 0; i + 1024 <= pcm.length; i += 1024) {
        const frame = await detector.analyze(btoa(pcm.slice(i, i + 1024)), i === 0);
        frames.push([frame.started, frame.speaking, frame.barge, frame.quietFrames]);
      }
      return frames;
    } finally { detector.close(); }
  }, probe.toString('base64'));
  // Golden decisions from the original Python Silero detector over the same 88 complete frames.
  assert.equal(frames.length, 88);
  assert.equal(createHash('sha256').update(JSON.stringify(frames)).digest('hex'),
    'a15e0fe772930b4ea10cf6e0d3ab01c28433552b525cbbb762138760340e5b05');
  const waitForReply = (text, timeout) => page.waitForFunction(expected =>
    window.voiceState.presentation?.messages.some(row => row.visible && row.text.includes(expected)), text, { timeout });
  await page.evaluate(() => {
    window.audioStops = 0; window.audioStarted = 0; window.activeAudio = new Set();
    window.voiceEvents = []; window.audioStartsByResponse = {}; window.startsAtCancel = {};
    window.beings.voice.onEvent(event => {
      window.voiceEvents.push(event);
      if (event.type === 'audio.delta' || event.type === 'audio.done') window.lastAudioResponse = event.responseId;
      if (event.type === 'playback.cancelled' && event.responseId)
        window.startsAtCancel[event.responseId] = window.audioStartsByResponse[event.responseId] || 0;
    });
    const start = AudioBufferSourceNode.prototype.start, stop = AudioBufferSourceNode.prototype.stop;
    AudioBufferSourceNode.prototype.start = function (...args) {
      window.audioStarted++;
      this.voiceResponseId = window.lastAudioResponse;
      window.audioStartsByResponse[this.voiceResponseId] = (window.audioStartsByResponse[this.voiceResponseId] || 0) + 1;
      window.activeAudio.add(this); this.addEventListener('ended', () => window.activeAudio.delete(this));
      return start.apply(this, args);
    };
    AudioBufferSourceNode.prototype.stop = function (...args) {
      window.audioStops++; window.activeAudio.delete(this); return stop.apply(this, args);
    };
  });
  const errors = []; page.on('pageerror', error => errors.push(error.message));
  assert.equal(await page.getByRole('button', { name: '语音设置' }).count(), 0);
  assert.equal(await page.locator('input[type=password]').count(), 0);
  await page.evaluate(() => {
    navigator.mediaDevices.getUserMedia = async () => { throw new DOMException('Fixture has no microphone', 'NotFoundError'); };
  });
  await page.getByRole('button', { name: '开启语音通话' }).click();
  await page.getByText('麦克风暂不可用，正在重试…', { exact: true }).waitFor({ timeout: 60000 });
  await page.getByText('麦克风不可用，请检查输入设备', { exact: true }).waitFor();
  assert.equal(audioInputs, 0);
  await page.evaluate(() => {
    delete navigator.mediaDevices.getUserMedia;
    navigator.mediaDevices.dispatchEvent(new Event('devicechange'));
  });
  await page.getByText('正在聆听', { exact: true }).waitFor({ timeout: 60000 });
  needTurn = true;
  await page.getByText('你：hello', { exact: true }).waitFor();
  await waitForReply('你好，实时语音已经连接。');
  assert.equal(await page.locator('.voice-panel, .voice-popover').count(), 0);
  assert.ok(audioInputs > 0, 'AudioWorklet sends microphone PCM');
  await page.waitForFunction(() => window.audioStarted > 0 && window.activeAudio.size === 0);
  const [provider] = wss.clients;
  const draft = { item_id: 'discarded-draft' };
  provider.send(JSON.stringify({ ...draft, type: 'conversation.item.input_audio_transcription.started' }));
  provider.send(JSON.stringify({ ...draft, type: 'conversation.item.input_audio_transcription.delta', delta: '临时转写' }));
  await page.getByText('你：临时转写', { exact: true }).waitFor();
  provider.send(JSON.stringify({ ...draft, type: 'conversation.item.input_audio_transcription.failed' }));
  await page.getByText('你：hello', { exact: true }).waitFor();
  assert.equal(await page.evaluate(() => window.voiceState.userTranscripts.some(row => row.text === '临时转写')), false);
  mode = 'delegate'; needTurn = true;
  await page.waitForFunction(() => window.voiceState.task.includes('任务已经由 Being 完成。'));
  await waitForReply('任务已经由 Being 完成。', 55000);
  assert.equal(requests.length, 1); assert.equal(requests[0].scene_id, 'fixture-scene');
  assert.ok(speech.some(text => text.includes('任务已经由 Being 完成。')));
  await page.waitForFunction(() => window.voiceEvents.some(event => event.type === 'task.notification.delivered'), undefined, { timeout: 55000 });
  assert.equal(speech.filter(text => text === '任务已经由 Being 完成。').length, 1, 'Long results are delivered without restarting TTS');
  assert.equal(await page.evaluate(() => window.audioStops), 0, 'Long speech reaches playback.ended without being cleared');
  mode = 'hold'; needTurn = true;
  await waitForReply('正在播报长回复');
  await page.waitForFunction(() => window.activeAudio.size > 0);
  const stops = await page.evaluate(() => window.audioStops);
  const heldStarts = await page.evaluate(id => window.audioStartsByResponse[id], heldResponse);
  assert.ok(heldStarts > 0);
  await page.getByRole('button', { name: '发送文字打断' }).click();
  // The text request can start new speech immediately; only the interrupted response must stop.
  await page.waitForFunction(({ stops, id }) => window.audioStops > stops &&
    [...window.activeAudio].every(source => source.voiceResponseId !== id), { stops, id: heldResponse });
  await page.waitForFunction(() => window.textResult?.includes('任务已经由 Being 完成。'));
  assert.ok(cancelledResponses.includes(heldResponse), 'Text cancels provider speech generation');
  assert.deepEqual(stoppedTasks, [], 'Interrupting speech does not stop Being tasks');
  assert.equal(requests.length, 2); assert.ok(requests[1].message.startsWith('文字打断'));
  assert.equal(await page.evaluate(id => window.audioStartsByResponse[id], heldResponse), heldStarts, 'Late audio cannot restart interrupted playback');
  await page.getByRole('button', { name: '暂停收音', exact: true }).click();
  await page.getByRole('button', { name: '恢复收音', exact: true }).waitFor();
  await page.getByRole('button', { name: '打断回复' }).click();
  const screenshot = await app.evaluate(async ({ BrowserWindow }) => {
    const image = await BrowserWindow.getAllWindows()[0].capturePage(undefined, { stayHidden: true, stayAwake: true });
    return image.toPNG().toString('base64');
  });
  await writeFile(path.join(root, 'milestone.png'), Buffer.from(screenshot, 'base64'));
  await page.getByRole('button', { name: '结束语音通话', exact: true }).click();
  await page.getByRole('button', { name: '开启语音通话' }).waitFor();
  if (!external) {
    const offset = await page.evaluate(() => window.voiceEvents.length);
    await page.getByRole('button', { name: '开启语音通话' }).click();
    await page.getByText('正在聆听', { exact: true }).waitFor();
    mode = 'delegate'; needTurn = true;
    const playingTask = await page.waitForFunction(from => {
      const event = window.voiceEvents.slice(from).find(e => e.type === 'response.started' && e.origin === 'announcement');
      if (event && [...window.activeAudio].some(source => source.voiceResponseId === event.responseId))
        return { taskId: event.taskIds[0], responseId: event.responseId };
    }, offset);
    const { taskId, responseId } = await playingTask.jsonValue();
    const spokenResults = speech.filter(text => text === '任务已经由 Being 完成。').length;
    const beforeInterruption = await page.evaluate(() => window.voiceEvents.length);
    lateResponseId = responseId;
    mode = '换个话题'; needTurn = true;
    await page.waitForFunction(({ taskId, responseId }) =>
      window.voiceEvents.some(e => e.task?.id === taskId && e.task.notificationStatus === 'interrupted') &&
      [...window.activeAudio].every(source => source.voiceResponseId !== responseId), { taskId, responseId });
    await waitForReply('收到你的问题：换个话题。');
    await page.waitForFunction(id => window.audioStartsByResponse[id] > 0, lastInputReplyId);
    const forwardedAudio = await page.evaluate(from => {
      const events = window.voiceEvents.slice(from);
      const transcript = events.findIndex(e => e.type === 'transcript.final' && e.role === 'user');
      return events.slice(transcript + 1).filter(e => e.type === 'audio.delta').map(e => e.responseId);
    }, beforeInterruption);
    assert.deepEqual(forwardedAudio, [lastInputReplyId], 'Only the new reply’s single audio packet reached the renderer');
    assert.equal(lateResponseId, '', 'The fixture sent late audio from the interrupted result');
    const startsAtCancel = await page.evaluate(id => window.startsAtCancel[id], responseId);
    assert.ok(startsAtCancel > 0, 'The task was audibly in progress before the new turn');

    const readyCount = await page.evaluate(() => window.voiceEvents.filter(e => e.type === 'voice.ready').length);
    [...wss.clients][0].terminate();
    await page.waitForFunction(count => window.voiceEvents.filter(e => e.type === 'voice.ready').length > count, readyCount);
    await page.getByRole('button', { name: '结束语音通话', exact: true }).click();
    await page.getByRole('button', { name: '开启语音通话' }).click();
    await page.getByText('正在聆听', { exact: true }).waitFor();
    const beforeNewTurn = await page.evaluate(() => window.voiceEvents.length);
    mode = '新的问题'; needTurn = true;
    await page.getByText('你：新的问题', { exact: true }).waitFor();
    await waitForReply('收到你的问题：新的问题。');
    await page.waitForFunction(from => window.voiceEvents.slice(from).some(e =>
      e.type === 'audio.delta' && window.audioStartsByResponse[e.responseId] > 0), beforeNewTurn);
    assert.equal(speech.filter(text => text === '任务已经由 Being 完成。').length, spokenResults);
    assert.equal(await page.evaluate(id => window.audioStartsByResponse[id], responseId), startsAtCancel,
      'No late audio from the interrupted result started playback');
    assert.deepEqual(stoppedTasks, []);
    await page.getByRole('button', { name: '结束语音通话', exact: true }).click();
    console.log('PASS: An ordinary new turn interrupts task audio, accepts its own reply, and does not replay the old result on reconnect or redial.');
  }
  if (external) {
    await page.getByRole('button', { name: '开启语音通话' }).click();
    await page.getByText('正在聆听', { exact: true }).waitFor();
    await stopManual();
    await page.getByRole('button', { name: '开启语音通话' }).waitFor();
    assert.equal(await startManual(Number(new URL(origin).port)), origin);
    mode = 'hello';
    const previousSpeech = speech.length;
    await page.getByRole('button', { name: '开启语音通话' }).click();
    await page.getByText('正在聆听', { exact: true }).waitFor();
    needTurn = true;
    await page.getByText('你：hello', { exact: true }).waitFor();
    await waitForReply('你好，实时语音已经连接。');
    assert.ok(speech.length > previousSpeech, 'Audio works again after a manual Gateway restart');
    await app.close(); app = undefined;
    assert.equal(manual.exitCode, null); assert.equal(manual.signalCode, null);
    const status = await fetch(origin + '/api/dev/status');
    assert.equal(status.status, 200, 'Closing Desktop leaves the manual Gateway running');
    console.log('PASS: Manual Gateway restart/reconnect and independent process lifetime.');
  }
  assert.deepEqual(errors, []);
  console.log('PASS: Electron microphone, Silero VAD, realtime voice, Being task playback, text interruption, mute and hangup.');
} finally {
  if (app) await app.close();
  await stopManual();
  await vite.close();
  for (const client of wss.clients) client.terminate();
  await new Promise(resolve => wss.close(resolve));
  server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
}
