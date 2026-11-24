const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { createInterface } = require('node:readline');
const { randomUUID } = require('node:crypto');
const path = require('node:path');

async function main() {
  const resources = process.argv[2];
  const directory = path.join(resources, 'audio-gateway');
  const binary = path.join(directory, process.platform === 'win32' ? 'AudioAgentGateway.exe' : 'AudioAgentGateway');
  const gateway = spawn(binary, [], { cwd: directory, stdio: ['pipe', 'pipe', 'inherit'], windowsHide: true, timeout: 60_000 });
  const exited = once(gateway, 'close');
  const lines = createInterface({ input: gateway.stdout });
  try {
    const token = randomUUID();
    const ready = Promise.race([once(lines, 'line'), exited.then(([code]) => { throw new Error('Packaged Gateway exited before ready: ' + code); })]);
    gateway.stdin.write(JSON.stringify({ data_dir: path.join(process.cwd(), 'gateway-data'),
      access_token: token, being_endpoint: 'http://127.0.0.1:1', being_token: 'fixture', being_scene: { scene_id: 'fixture' } }) + '\n');
    const [line] = await ready;
    const event = JSON.parse(line);
    assert.equal(event.type, 'gateway.ready');
    const request = { headers: { Authorization: 'Bearer ' + token }, signal: AbortSignal.timeout(10_000) };
    const health = await fetch(event.origin + '/api/health', request);
    assert.equal(health.status, 200);
    assert.equal((await health.json()).status, 'ready');
    const history = await fetch(event.origin + '/api/heart/history', request);
    assert.equal(history.status, 200);
    assert.deepEqual((await history.json()).messages, []);
  } finally {
    lines.close();
    gateway.stdin.end();
    assert.deepEqual(await exited, [0, null]);
  }
  console.log('PASS relocated Gateway: bundled executable loaded Silero and served authenticated history.');
}
main().catch(error => { console.error(error); process.exitCode = 1; });
