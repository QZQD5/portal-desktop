// Built web entry, a real AudioWorklet + Silero worker, synthetic mic, and a
// provider fixture. No user audio, credentials, or remote service.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";
import { createServer } from "vite";

const local = { host: "127.0.0.1", port: 0 };
let dev, packaged, browser, origin;
try {
  if (process.argv.includes("--dev")) {
    dev = await (await createServer({ configFile: "vite.web.config.mjs", server: local })).listen();
    origin = `http://127.0.0.1:${dev.httpServer.address().port}`;
  } else {
    packaged = spawn(process.execPath, ["out/web/server.mjs"], {
      env: { ...process.env, HOST: local.host, PORT: "0" }, stdio: ["ignore", "pipe", "inherit"], windowsHide: true,
    });
    origin = await Promise.race([
      once(packaged.stdout, "data").then(([data]) => data.toString().match(/http:\/\/127\.0\.0\.1:\d+/)?.[0]),
      once(packaged, "exit").then(([code]) => { throw new Error("Web server exited: " + code); }),
    ]);
    assert.ok(origin, "Standalone web server printed its local address");
  }
  browser = await chromium.launch({ channel: "chrome", headless: true,
    args: ["--use-fake-device-for-media-stream", "--use-fake-ui-for-media-stream"] });
  const page = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true,
    permissions: ["microphone"] });
  const failures = [];
  page.on("console", msg => { if (msg.type() === "error") failures.push(msg.text()); });
  page.on("pageerror", error => failures.push(error.message));
  page.on("requestfailed", request => failures.push(request.url() + " " + request.failure()?.errorText));
  page.on("response", response => { if (response.status() >= 400) failures.push(response.status() + " " + response.url()); });
  await page.addInitScript(endpoint => {
    if (!localStorage.getItem("town-web:connection")) localStorage.setItem("town-web:connection",
      JSON.stringify({ endpoint, token: "fixture.voice-token-0001" }));
    const original = navigator.mediaDevices.getUserMedia.bind(navigator.mediaDevices);
    window.fixtureMicrophones = [];
    navigator.mediaDevices.getUserMedia = async options => {
      const media = await original(options);
      window.fixtureMicrophones.push(...media.getAudioTracks());
      if (window.fixtureDelayPermission) await new Promise(resolve => { window.fixtureGrant = resolve; });
      return media;
    };
  }, origin + "/willow");
  let taskRequests = 0;
  await page.route("**/*", route => {
    const url = new URL(route.request().url());
    if (url.origin !== origin) return route.abort();
    if (url.pathname === "/_web/config") return route.fulfill({ json: { loomEndpoint: origin + "/willow" } });
    if (url.pathname === "/loom-api/api/stream/active") return route.fulfill({ status: 204 });
    if (url.pathname === "/loom-api/api/history") return route.fulfill({ json: { messages: [] } });
    if (url.pathname === "/loom-api/api/chat/stream" && route.request().method() === "POST") {
      assert.equal(url.search, "");
      assert.equal(route.request().headers().authorization, "Bearer fixture.voice-token-0001");
      assert.ok(route.request().postDataJSON().scene_id.startsWith("web-"));
      taskRequests++;
      return route.fulfill({ contentType: "text/event-stream",
        body: 'event: content_block_delta\ndata: {"delta":{"text":"任务结果"}}\n\nevent: message_stop\ndata: {}\n\n' });
    }
    if (url.pathname.match(/^\/(town|loom)-api\//)) return route.fulfill({ json: { being_name: "Willow" } });
    return route.continue();
  });
  let audioFrames = 0, connections = 0, provider, disconnected, seeded = [], voiceToken = "fixture.voice-token-0001";
  await page.routeWebSocket("**/v1/realtime/*", socket => {
    provider = socket;
    connections++;
    disconnected = new Promise(resolve => socket.onClose(resolve));
    assert.equal(new URL(socket.url()).search, "");
    let authenticated = false;
    socket.onMessage(message => {
      if (!authenticated) {
        assert.equal(message, voiceToken);
        authenticated = true;
        socket.send('{"type":"web.voice.ready"}');
        return;
      }
      const event = JSON.parse(message);
      if (event.type === "session.create") socket.send('{"type":"session.created","session":{"id":"fixture"}}');
      if (event.type === "conversation.item.create" && event.items?.[0]?.type === "message")
        seeded = event.items.map(item => [item.role, item.content[0].text]);
      if (event.type === "input_audio_buffer.append") { assert.ok(event.audio.length > 0); audioFrames++; }
    });
  });
  await page.goto(origin);
  const frame = page.frameLocator('iframe[title="Being 对话"]');
  await frame.getByRole("button", { name: "开启语音通话" }).click();
  await frame.getByRole("button", { name: "暂停收音" }).waitFor({ timeout: 30000 }).catch(async error => {
    console.error(await frame.locator("body").innerText(), failures);
    throw error;
  });
  const start = Date.now();
  while (!audioFrames && Date.now() - start < 12000) await new Promise(resolve => setTimeout(resolve, 100));
  assert.ok(audioFrames > 0, "Microphone PCM reached the provider over the foreground voice connection");
  const send = event => provider.send(JSON.stringify(event));
  send({ type: "conversation.item.input_audio_transcription.completed", item_id: "fixture-turn", transcript: "帮我总结" });
  send({ type: "response.output_text.done", response_id: "fixture-tool", text: "好的，我来总结。" });
  send({ type: "response.function_call_arguments.done", response_id: "fixture-tool", call_id: "fixture-call",
    name: "spawn_thinking", arguments: { objective: "总结这段对话" } });
  await page.waitForFunction(() => Object.keys(localStorage).filter(key => key.endsWith(":tasks"))
    .some(key => JSON.parse(localStorage[key]).some(task => task.result === "任务结果" && task.status === "completed")));
  assert.equal(taskRequests, 1);
  await page.getByRole("navigation", { name: "底部导航", exact: true }).getByRole("button", { name: "设置" }).click();
  const waitForHangup = () => Promise.race([disconnected,
    delay(10000, undefined, { ref: false }).then(() => { throw new Error("Foreground call failed to close"); })]);
  await waitForHangup();
  assert.deepEqual(await frame.locator("body").evaluate(() => window.fixtureMicrophones.map(track => track.readyState)),
    ["ended"], "Leaving chat stops the microphone track");
  await page.getByRole("navigation", { name: "底部导航", exact: true }).getByRole("button", { name: "对话" }).click();
  await frame.getByRole("button", { name: "开启语音通话" }).waitFor();
  // Hold a real MediaStream as if the platform's permission sheet were still open.
  // Headless Chrome does not background other tabs, so deliver the same DOM signal explicitly.
  const chat = page.frames().find(child => new URL(child.url()).pathname.endsWith("/chat.html"));
  await chat.evaluate(() => { window.fixtureDelayPermission = true; });
  await frame.getByRole("button", { name: "开启语音通话" }).click();
  await chat.waitForFunction(() => !!window.fixtureGrant);
  await chat.evaluate(() => {
    Object.defineProperty(document, "hidden", { configurable: true, value: true });
    document.dispatchEvent(new Event("visibilitychange"));
    window.fixtureGrant();
  });
  await chat.waitForFunction(() => window.fixtureMicrophones.length === 2 && window.fixtureMicrophones.every(track => track.readyState === "ended"));
  await frame.getByRole("button", { name: "开启语音通话" }).waitFor();
  await waitForHangup();
  assert.equal(connections, 2, "Granting microphone permission after backgrounding cannot reopen the call");
  await chat.evaluate(() => {
    delete document.hidden;
    const resume = AudioContext.prototype.resume, post = Worker.prototype.postMessage, terminate = Worker.prototype.terminate;
    AudioContext.prototype.resume = function() { window.fixtureContext = this; return resume.call(this); };
    Worker.prototype.postMessage = function(...args) {
      if (args[0]?.reset && args[0]?.audio === "") { window.fixtureWarmup = this; return; }
      return post.apply(this, args);
    };
    Worker.prototype.terminate = function() {
      if (this === window.fixtureWarmup) window.fixtureWorkerClosed = true;
      return terminate.call(this);
    };
  });
  await frame.getByRole("button", { name: "开启语音通话" }).click();
  await chat.waitForFunction(() => !!window.fixtureWarmup);
  await chat.evaluate(() => {
    Object.defineProperty(document, "hidden", { configurable: true, value: true });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await chat.waitForFunction(() => window.fixtureWorkerClosed && window.fixtureContext.state === "closed");
  assert.equal(connections, 2, "Backgrounding during Silero warmup never opens another provider connection");
  // Reload the ordinary chat view and reconnect. Both records are still in the
  // real IndexedDB history cache, while the upstream history fixture stays empty.
  await page.reload();
  await frame.getByText("帮我总结", { exact: true }).waitFor();
  assert.equal(await frame.getByText("好的，我来总结。", {exact:true}).count(), 1);
  seeded = [];
  await frame.getByRole("button", { name: "开启语音通话" }).click();
  await frame.getByRole("button", { name: "暂停收音" }).waitFor();
  assert.deepEqual(seeded, [["user", "帮我总结"], ["assistant","好的，我来总结。"]]);
  await frame.getByRole("button", { name: "结束语音通话" }).click();
  await waitForHangup();
  const original = await page.evaluate(() => {
    const key = Object.keys(localStorage).find(key => key.startsWith("town-web:scenes:"));
    const value = localStorage[key], group = JSON.parse(value);
    group.active = "fixture-other-scene";
    group.scenes.push({ scene_id: group.active, scene_meta: { client:"town-web", scene_label:"另一场景" } });
    localStorage[key] = JSON.stringify(group);
    return { key, value };
  });
  const emptyArchive = async () => {
    await page.reload();
    seeded = [];
    await frame.getByRole("button", { name: "开启语音通话" }).click();
    await frame.getByRole("button", { name: "暂停收音" }).waitFor();
    assert.deepEqual(seeded, []);
    assert.equal(await frame.getByText("帮我总结", { exact:true }).count(), 0);
    await frame.getByRole("button", { name: "结束语音通话" }).click();
    await waitForHangup();
  };
  await emptyArchive();
  await page.evaluate(({key,value}) => {
    localStorage[key] = value;
    const binding = JSON.parse(localStorage["town-web:connection"]);
    localStorage["town-web:connection"] = JSON.stringify({...binding, token:"fixture.voice-token-0002"});
  }, original);
  voiceToken = "fixture.voice-token-0002";
  await emptyArchive();
  await page.getByRole("button", { name: "Being 连接设置" }).click();
  await page.getByRole("button", { name: "断开 Being" }).click();
  await page.waitForFunction(async () => {
    const {name} = (await indexedDB.databases()).find(db => db.name?.startsWith("loom-history-gateway-v2-"));
    const db = await new Promise(resolve => { const r = indexedDB.open(name); r.onsuccess = () => resolve(r.result); });
    const count = await new Promise(resolve => {
      const r = db.transaction("meta").objectStore("meta").count(IDBKeyRange.bound(["voice"], ["voice", []]));
      r.onsuccess = () => resolve(r.result);
    });
    db.close();
    return count === 0;
  });
  await page.evaluate(endpoint => { localStorage.setItem("town-web:connection",
    JSON.stringify({ endpoint, token:"fixture.voice-token-0001" })); }, origin + "/willow");
  await page.route("**/web-config.json", route => route.fulfill({ json: { mode: "static", apiBase: "" } }));
  await page.reload();
  await frame.getByRole("textbox", { name: "消息输入框" }).waitFor();
  assert.equal(await frame.getByRole("button", { name: "开启语音通话" }).count(), 0);
  console.log(`PASS: Mobile Web call, mic → worklet → VAD → authenticated WS (${audioFrames} PCM packets), Being task SSE, foreground stop`);
} finally {
  await browser?.close();
  await dev?.close();
  if (packaged && packaged.exitCode === null) {
    const closed = once(packaged, "exit");
    packaged.kill();
    await closed;
  }
}
