"""Real Python child + HTTP + WebSocket; only cloud endpoints are fixtures."""
import asyncio
import json
import os
import socket
import sys

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, Response
import uvicorn
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from conftest import Client, SeeduplexFixture
from gateway.__main__ import desktop_settings

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("mode", ["child", "development"])
async def test_desktop_child_voice_being_and_spoken_result(tmp_path, mode):
    provider = SeeduplexFixture()
    heart = FastAPI()
    submissions = []

    @heart.get('/willow/api/status')
    async def status(): return {"being_name": "Willow"}

    @heart.get('/willow/api/stream/active')
    async def active(): return Response(status_code=204)

    @heart.post('/willow/api/chat/stream')
    async def chat(body: dict):
        submissions.append(body)
        async def stream():
            for kind, data in [('meta', {'stream_id': 'test-stream'}),
                               ('content_block_delta', {'delta': {'text': 'Being task result'}}), ('message_stop', {})]:
                yield f'event: {kind}\ndata: {json.dumps(data)}\n\n'
        return StreamingResponse(stream(), media_type='text/event-stream')

    sock = socket.socket(); sock.bind(('127.0.0.1', 0)); sock.listen()
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(heart, log_level='error'))
    serving = asyncio.create_task(server.serve(sockets=[sock]))
    child = stderr = None
    try:
        while not server.started: await asyncio.sleep(.01)
        async with serve(provider.handle, '127.0.0.1', 0) as upstream:
            token = 'desktop-private-token-12345678901234567890'
            config = dict(data_dir=str(tmp_path), access_token=token, being_endpoint=f'http://127.0.0.1:{port}/willow',
                          being_token='heart-private-token', being_scene={'scene_id': 'desktop-scene'},
                          doubao_url=f'ws://127.0.0.1:{upstream.sockets[0].getsockname()[1]}')
            desktop_settings(config)
            command = [os.environ['GATEWAY_EXECUTABLE']] if os.getenv('GATEWAY_EXECUTABLE') else [sys.executable, '-u', '-m', 'gateway']
            if mode == 'development':
                command = [sys.executable, '-u', '-m', 'gateway', '--dev', '--port', '0', '--data-dir', str(tmp_path / 'dev')]
                config.pop('access_token')
            headers = {'Authorization': 'Bearer ' + token} if mode == 'child' else {}
            child = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                env={**os.environ, 'PYTHONIOENCODING': 'utf-8', 'DOUBAO_API_KEY': 'voice-private-key'})
            stderr = asyncio.create_task(child.stderr.read())
            if mode == 'child':
                child.stdin.write((json.dumps(config) + '\n').encode()); await child.stdin.drain()
            async with asyncio.timeout(40):
                line = await child.stdout.readline()
                if not line: raise AssertionError((await stderr).decode())
                ready = json.loads(line)
            assert ready['type'] == 'gateway.ready'
            origin = ready['origin']
            async with httpx.AsyncClient() as http:
                assert (await http.get(origin + '/api/health')).status_code == (401 if mode == 'child' else 503)
                if mode == 'development':
                    assert (await http.post(origin + '/api/dev/configure', headers=headers, json=config)).status_code == 200
                assert (await http.get(origin + '/', headers=headers)).status_code == 404
            async with connect(origin.replace('http', 'ws') + '/api/realtime', additional_headers=headers) as ws:
                client = Client(ws)
                await client.send('session.hello', protocol={'min': '7.0.0', 'max': '7.0.0'},
                    client={'type': 'desktop', 'instance_id': 'fixture'},
                    capabilities=['input.audio', 'input.text', 'playback.receipts', 'tasks.commands'],
                    connection={'voice_enabled': True, 'input_enabled': True, 'output_enabled': True})
                await client.wait('voice.ready')
                await client.speak(provider, 'hello')
                await client.wait('transcript.final', lambda e: e.get('role') == 'assistant' and 'hello' in e.get('content', ''))
                await client.wait('audio.done')
                await client.speak(provider, 'delegate')
                completed = await client.wait('task.completed')
                assert completed['task']['result'] == 'Being task result'
                await client.wait('transcript.final', lambda e: 'Being task result' in e.get('content', ''), timeout=15)
                assert len(submissions) == 1 and submissions[0]['scene_id'] == 'desktop-scene'
            if mode == 'child':
                child.stdin.write(b'\n'); await child.stdin.drain()
                assert await asyncio.wait_for(child.wait(), 15) == 0
            else:
                child.stdin.close()
                async with httpx.AsyncClient() as http:
                    assert (await http.get(origin + '/api/dev/status')).json()['configured'] is True
    finally:
        if child and child.returncode is None:
            child.kill(); await child.wait()
        if stderr:
            await stderr
        for job in provider.jobs: job.cancel()
        await asyncio.gather(*provider.jobs, return_exceptions=True)
        server.should_exit = True
        await serving
        sock.close()
