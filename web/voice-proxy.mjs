import WebSocket, { WebSocketServer } from "ws";

const endpoint = "wss://101.132.86.87/v1/realtime/";

// A browser WebSocket cannot set an Authorization header. Accept its credential
// in the first frame, then authenticate the fixed Being voice connection.
export function installVoiceProxy(server, upstream = endpoint) {
  const sockets = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024, perMessageDeflate: false });
  server.on("upgrade", (request, socket, head) => {
    if (!request.url?.startsWith("/v1/realtime/")) return;
    const route = /^\/v1\/realtime\/([A-Za-z0-9_-]{1,128})$/.exec(request.url);
    const host = request.headers.host;
    const origin = request.headers.origin;
    const localHttp = /^(?:localhost|127\.0\.0\.1|\[::1\])(?::\d+)?$/.test(host || "") && origin === `http://${host}`;
    if (!route || !host || (origin !== `https://${host}` && !localHttp) ||
        request.headers["sec-fetch-site"] === "cross-site") {
      socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
      return;
    }
    sockets.handleUpgrade(request, socket, head, client => {
      let provider;
      const timeout = setTimeout(() => client.close(1008, "Voice authentication timed out"), 10000);
      client.on("error", () => client.terminate());
      client.once("close", () => {
        clearTimeout(timeout);
        if (provider) provider.terminate();
      });
      client.once("message", (data, binary) => {
        clearTimeout(timeout);
        const token = !binary && data.length <= 2048 && data.toString();
        if (!token || !/^[A-Za-z0-9._~-]{16,2048}$/.test(token)) {
          client.close(1008, "Invalid Loom credential");
          return;
        }
        provider = new WebSocket(upstream + route[1], {
          headers: { Authorization: `Bearer ${token}` },
          followRedirects: false, handshakeTimeout: 35000, maxPayload: 4 * 1024 * 1024,
          perMessageDeflate: false,
        });
        provider.on("open", () => client.send(JSON.stringify({ type: "web.voice.ready" })));
        provider.on("unexpected-response", (_req, res) => {
          res.resume();
          client.close([401, 403].includes(res.statusCode) ? 1008 : 1011, `Voice upstream HTTP ${res.statusCode}`);
          provider.terminate();
        });
        provider.on("error", () => client.close(1011, "Voice upstream connection failed"));
        provider.on("close", code => client.close(code === 1008 ? 1008 : 1000, "Voice upstream closed"));
        for (const [from, to] of [[client, provider], [provider, client]]) {
          from.on("message", (payload, isBinary) => {
            if (isBinary || to.readyState !== WebSocket.OPEN) {
              client.close(1008, "Expected a connected voice text stream");
              return;
            }
            // Pace the source even when the mobile downlink temporarily stalls.
            from.pause();
            to.send(payload, { binary: false }, error => error ? from.terminate() : from.resume());
          });
        }
      });
    });
  });
  server.on("close", () => sockets.close());
}
