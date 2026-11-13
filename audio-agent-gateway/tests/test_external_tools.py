import asyncio
import json
import socket
import sys
from pathlib import Path

import httpx
import pytest
import uvicorn
from mcp.server.fastmcp import FastMCP

from conftest import unit_settings as Settings
from gateway.external_tools import ExternalTools, endpoint
from gateway.search import search


async def test_mcp_real_stdio_discovery_allowlist_call_and_close(tmp_path):
    config = tmp_path / "mcp.json"
    config.write_text(
        json.dumps(
            {
                "version": 1,
                "servers": {
                    "fixture": {
                        "enabled": True,
                        "transport": {
                            "type": "stdio",
                            "command": sys.executable,
                            "args": [str(Path(__file__).with_name("fake_frontend_mcp.py"))],
                        },
                        "tools": {"echo": {"enabled": True, "maxCallsPerTurn": 1}},
                    }
                },
            }
        )
    )
    tools = ExternalTools(Settings(frontend_mcp_config=str(config)))
    try:
        await tools.start()
        assert tools.health("mcp")["ok"], tools.health("mcp")
        assert [item["name"] for item in tools.definitions()] == ["mcp__fixture__echo"]
        turn = {}
        result = await tools.execute("mcp__fixture__echo", {"value": "real pipes"}, turn)
        assert json.loads(result["text"]) == {"echo": "real pipes"}
        with pytest.raises(ValueError, match="call limit"):
            await tools.execute("mcp__fixture__echo", {"value": "second"}, turn)
    finally:
        await tools.close()
    assert all(job.done() for job in tools.jobs)
    assert not tools.sessions


async def test_mcp_real_http_and_search_provider(tmp_path):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    mcp = FastMCP("http-fixture", host="127.0.0.1", port=port)

    @mcp.tool()
    def web_search(q: str, top_k: int = 5) -> dict:
        return {
            "results": [
                {"title": q, "url": "https://example.org/source", "snippet": "source", "count": top_k}
            ]
        }

    config = tmp_path / "http.json"
    url = f"http://127.0.0.1:{port}/mcp"
    config.write_text(
        json.dumps(
            {
                "version": 1,
                "servers": {
                    "http": {"enabled": True, "url": url, "tools": {"web_search": {"enabled": True}}}
                },
            }
        )
    )
    settings = Settings(frontend_mcp_config=str(config), web_search_provider="mcp", web_search_mcp_url=url)
    server = uvicorn.Server(uvicorn.Config(mcp.streamable_http_app(), log_level="error"))
    job = asyncio.create_task(server.serve(sockets=[sock]))
    tools = ExternalTools(settings)
    try:
        async with asyncio.timeout(5):
            while not server.started:
                await asyncio.sleep(0.01)
        await tools.start()
        assert tools.health("mcp")["ok"], tools.health("mcp")
        result = await tools.execute("mcp__http__web_search", {"q": "http transport"}, {})
        assert "http transport" in result["text"]
        result = await search(settings, "test sources", 3, None)
        assert (
            result["results"][0]["title"] == "test sources"
            and result["citations"][0]["url"] == "https://example.org/source"
        )
    finally:
        await tools.close()
        server.should_exit = True
        await job


async def test_bing_parser_and_disabled_search():
    def fetch(url):
        assert "bing.com" in url
        return (
            url,
            '<li class="b_algo"><h2><a href="https://example.org">A<strong>B</strong></a></h2><div class="b_caption"><p>Summary</p></div></li>',
        )

    result = await search(Settings(web_search_provider="bing"), "query", 5, fetch)
    assert result["results"][0]["title"] == "AB"
    with pytest.raises(ValueError, match="disabled"):
        await search(Settings(web_search_provider="none"), "query", 5, fetch)


def api_config(tmp_path, **overrides):
    document = {
        "openapi": "3.1.0",
        "servers": [{"url": "https://example.test/v1"}],
        "paths": {
            "/things/{id}": {
                "get": {
                    "operationId": "readThing",
                    "parameters": [
                        {"name": "id", "in": "path", "required": True, "schema": {"type": "string"}},
                        {
                            "name": "tags",
                            "in": "query",
                            "schema": {"type": "array", "items": {"type": "string"}},
                        },
                    ],
                    "responses": {"200": {"schema": {"$ref": "#/components/schemas/Recursive"}}},
                }
            }
        },
        "components": {"schemas": {"Recursive": {"$ref": "#/components/schemas/Recursive"}}},
    }
    (tmp_path / "api.json").write_text(json.dumps(document))
    config = tmp_path / "openapi.json"
    config.write_text(
        json.dumps(
            {
                "version": 1,
                "apis": {
                    "demo": {
                        "enabled": True,
                        "document": "api.json",
                        "operations": {"readThing": {"enabled": True}},
                        **overrides,
                    }
                },
            }
        )
    )
    return config


async def test_openapi_mapping_and_bounded_redirect_free_requests(tmp_path, monkeypatch):
    config = api_config(tmp_path)
    tools = ExternalTools(Settings(frontend_openapi_config=str(config)))
    await tools.start()
    assert tools.health("openapi")["ok"], tools.health("openapi")
    assert tools.definitions()[0]["parameters"]["required"] == ["id"]
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json={"name": "demo"})

    real = httpx.AsyncClient
    monkeypatch.setattr(
        httpx, "AsyncClient", lambda **kwargs: real(transport=httpx.MockTransport(handle), **kwargs)
    )
    result = await tools.execute("openapi__demo__readThing", {"id": "a/b", "tags": ["one", "two"]}, {})
    assert result["structured_content"] == {"name": "demo"}
    assert requests[0].url.raw_path == b"/v1/things/a%2Fb?tags=one&tags=two"
    assert result["notice"]
    await tools.close()


@pytest.mark.parametrize(
    "url,headers",
    [
        ("http://remote.example", {}),
        ("http://127.0.0.1", {"authorization": "secret"}),
        ("file:///D:/secrets", {}),
        ("https://user:secret@host.test", {}),
    ],
)
def test_configured_tool_endpoint_validation(url, headers):
    with pytest.raises(ValueError):
        endpoint(url, headers)
