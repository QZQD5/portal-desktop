import httpx
import pytest
from starlette.requests import HTTPConnection

from gateway.access import authorized
from gateway.config import Settings, desktop_settings
from gateway.development import create_dev_app


async def test_dev_binding_is_local_bounded_and_isolated(tmp_path, being_service):
    app = create_dev_app(Settings(data_dir=tmp_path / "dev", development=True, doubao_api_key="gateway-private-key"))
    async with app.router.lifespan_context(app), httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://127.0.0.1",
    ) as http:
        assert (await http.get("/api/dev/status", headers={"Origin": "http://127.0.0.1"})).status_code == 401
        assert (await http.get("/api/dev/status")).json()["configured"] is False
        assert (await http.get("/api/health")).status_code == 503
        assert (await http.post("/api/dev/configure", content="x" * 65537)).status_code == 413
        assert (await http.post("/api/dev/configure", json=[])).status_code == 400
        config = {"being_endpoint": being_service, "being_token": "private-being-token",
                  "being_scene": {"scene_id": "scene-a"},
                  "data_dir": str(tmp_path / "must-not-use")}
        assert (await http.post("/api/dev/configure", json=config)).status_code == 200
        first = (await http.get("/api/health")).json()["gatewayInstanceId"]
        assert (await http.post("/api/dev/configure", json=config)).status_code == 200
        assert (await http.get("/api/health")).json()["gatewayInstanceId"] == first
        assert (await http.post("/api/dev/configure", json={**config, "being_endpoint": "http://remote.example"})).status_code == 400
        assert (await http.post("/api/dev/configure", json={**config, "doubao_url": {"bad": True}})).status_code == 400
        assert (await http.post("/api/dev/configure", json={**config, "doubao_url": "ws://localhost:bad"})).status_code == 400
        assert (await http.get("/api/health")).json()["gatewayInstanceId"] == first
        config["being_scene"] = {"scene_id": "scene-b"}
        assert (await http.post("/api/dev/configure", json=config)).status_code == 200
        assert (await http.get("/api/health")).json()["gatewayInstanceId"] != first
        assert len(list((tmp_path / "dev").glob("*/gateway.sqlite3"))) == 1
        assert not (tmp_path / "must-not-use").exists()
        response = (await http.get("/api/dev/status")).text
        assert "private-being-token" not in response and "private-voice-key" not in response


@pytest.mark.parametrize("kind", ["http", "websocket"])
def test_only_explicit_development_skips_token_and_still_requires_local_non_browser_access(kind):
    scope = {"type": kind, "scheme": "http" if kind == "http" else "ws", "path": "/api/realtime",
             "query_string": b"", "client": ("127.0.0.1", 1234), "headers": [(b"host", b"127.0.0.1:3102")]}
    dev = Settings(development=True)
    private = Settings(access_token="private-child-token-1234567890123456")
    assert authorized(HTTPConnection(scope), dev)
    assert not authorized(HTTPConnection(scope), Settings())
    assert not authorized(HTTPConnection(scope), private)
    headers = scope["headers"] + [(b"authorization", ("Bearer " + private.access_token).encode())]
    assert authorized(HTTPConnection({**scope, "headers": headers}), private)
    for forbidden in ({"client": ("192.0.2.1", 1234)},
                      {"headers": headers + [(b"origin", b"http://127.0.0.1")]},
                      {"headers": [(b"host", b"remote.example"), headers[-1]]}):
        request = HTTPConnection({**scope, **forbidden})
        assert not authorized(request, dev)
        assert not authorized(request, private)


def test_child_configuration_cannot_enable_development_through_payload(tmp_path):
    value = {"data_dir": str(tmp_path), "being_endpoint": "https://being.example/fixture", "being_token": "private",
             "being_scene": {"scene_id": "fixture"}, "doubao_api_key": "private", "development": True}
    with pytest.raises(ValueError, match="access_token"):
        desktop_settings(value)
    value["access_token"] = "private-child-token-1234567890123456"
    assert not desktop_settings(value).development


def test_provider_key_belongs_to_gateway_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("DOUBAO_API_KEY", "gateway-owned-key")
    value = {"data_dir": str(tmp_path), "being_endpoint": "https://being.example/fixture", "being_token": "private",
             "being_scene": {"scene_id": "fixture"}, "doubao_api_key": "must-not-override-gateway"}
    assert desktop_settings(value, development=True).doubao_api_key == "gateway-owned-key"
    monkeypatch.delenv("DOUBAO_API_KEY")
    assert desktop_settings(value, development=True).doubao_api_key == ""


async def test_missing_gateway_key_does_not_accept_a_desktop_key(tmp_path):
    app = create_dev_app(Settings(data_dir=tmp_path, development=True, doubao_api_key=""))
    async with app.router.lifespan_context(app), httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://127.0.0.1") as http:
        response = await http.post("/api/dev/configure", json={"being_endpoint": "https://being.example/fixture",
            "being_token": "private", "being_scene": {"scene_id": "fixture"}, "doubao_api_key": "must-not-use"})
        assert response.status_code == 200
        assert (await http.get("/api/local-history")).json()["messages"] == []
        assert "must-not-use" not in response.text
