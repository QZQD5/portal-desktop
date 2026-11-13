import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from .seeduplex import MODEL, URL, VOICE

ROOT = Path(__file__).resolve().parent.parent


def desktop_display_name(value):
    if not isinstance(value, str) or len(value) > 400 or "\x00" in value:
        raise ValueError("Invalid being_name")
    return " ".join(value.split())[:128] or None


@dataclass
class Settings:
    being_endpoint: str = ""
    being_token: str = ""
    being_scene: dict = field(default_factory=dict)
    host: str = "127.0.0.1"
    port: int = 3102
    doubao_url: str = URL
    doubao_api_key: str = field(default_factory=lambda: os.getenv("DOUBAO_API_KEY", "").strip())
    doubao_model: str = MODEL
    doubao_voice: str = VOICE
    data_dir: Path = field(default_factory=lambda: ROOT / ".runtime")
    backend_enabled: bool = True
    response_timeout: float = 60
    response_inactivity_timeout: float = 120
    speech_first_audio_timeout: float = 15
    speech_audio_inactivity_timeout: float = 20
    cancel_timeout: float = 1
    task_timeout: float = 0
    scheduled_task_timeout: float = 1800
    tool_timeout: float = 30
    announcement_ack_timeout: float = 120
    announcement_max_retries: int = 8
    announce_into_context: bool = True
    result_context_max_chars: int = 6000
    announcement_batch_window: float = 0.12
    announcement_max_batch_items: int = 8
    announcement_quiet: float = 0.35
    reminder_scheduler: bool = True
    reminder_stagger: float = 30
    frontend_prompt_dir: Path | None = None
    owner_id: str = "user_personal"
    access_token: str = ""
    development: bool = False
    max_concurrent_tasks: int = 4
    sleep_timeout: float = 0
    frontend_mcp_config: str = ""
    frontend_openapi_config: str = ""
    client_event_definitions: list[dict] = field(default_factory=list)
    max_sessions: int = 500
    session_ttl: float = 21600
    max_terminal_tasks: int = 100
    terminal_task_ttl: float = 86400
    pending_notification_ttl: float = 604800
    web_search_provider: str = "so360"
    web_search_mcp_url: str = ""
    web_search_mcp_token: str = ""
    web_search_mcp_tool: str = "web_search"
    disabled_tools: list[str] = field(default_factory=lambda: ["web_search", "fetch_url"])
    state_dir: Path | None = None
    portal_state_dir: Path | None = None
    workspace_dir: Path | None = None
    instance_id: str = ""
    legacy_history: list = field(default_factory=list)

    @property
    def workspace(self):
        return self.workspace_dir or self.data_dir / "workspace"

    @property
    def state(self):
        return self.state_dir or self.data_dir


def validate_setup(settings):
    if not isinstance(settings.doubao_url, str):
        raise ValueError("Invalid realtime voice provider URL")
    parsed = urlsplit(settings.doubao_url)
    if (
        parsed.scheme not in {"ws", "wss"} or not parsed.hostname
    ):
        raise ValueError("Invalid realtime voice provider URL")
    if settings.doubao_model != "1.2.6.1":
        raise ValueError("Unsupported voice model; expected 1.2.6.1")
    if parsed.username or parsed.password:
        raise ValueError("Provider URL must not contain credentials")
    _ = parsed.port
    if not 0 <= settings.port <= 65535:
        raise ValueError("Port must be 0-65535")

def validate_being_endpoint(endpoint):
    endpoint = endpoint.rstrip("/")
    url = urlsplit(endpoint)
    if (not url.hostname or (url.scheme != "https" and not (url.scheme == "http" and url.hostname in {"127.0.0.1", "localhost"}))
            or url.username or url.password or url.query or url.fragment):
        raise ValueError("Being requires an HTTPS endpoint (HTTP is allowed only on loopback)")
    _ = url.port
    return endpoint


def desktop_settings(value, *, development=False):
    if not isinstance(value, dict):
        raise ValueError("Desktop configuration must be an object")
    required = ("data_dir", "being_endpoint", "being_token")
    if not development:
        required += ("access_token",)
    for key in required:
        if not isinstance(value.get(key), str) or not value[key].strip():
            raise ValueError("Missing Desktop setting: " + key)
    if not Path(value["data_dir"]).is_absolute() or (not development and len(value["access_token"]) < 32):
        raise ValueError("Invalid Desktop data directory or access token")
    validate_being_endpoint(value["being_endpoint"])
    scene = value.get("being_scene")
    if not isinstance(scene, dict) or not isinstance(scene.get("scene_id"), str) or not 1 <= len(scene["scene_id"]) <= 128:
        raise ValueError("Desktop must bind a valid Being scene")
    settings = Settings(host="127.0.0.1", port=0, data_dir=Path(value["data_dir"]),
                        access_token="" if development else value["access_token"], development=development,
                        being_endpoint=value["being_endpoint"],
                        being_token=value["being_token"], being_scene=scene,
                        disabled_tools=["web_search", "fetch_url", "respond_permission", "respond_agent_input"])
    imports = value.get("legacy_history", [])
    if not isinstance(imports, list) or len(imports) > 500:
        raise ValueError("Invalid legacy history directories")
    for item in imports:
        if (not isinstance(item, dict) or not isinstance(item.get("path"), str)
                or not Path(item["path"]).is_absolute() or not isinstance(item.get("scene"), dict)):
            raise ValueError("Invalid legacy history directory")
    settings.legacy_history = imports
    portal_dir = value.get("portal_state_dir")
    if portal_dir:
        if not isinstance(portal_dir, str) or not Path(portal_dir).is_absolute():
            raise ValueError("Invalid portal state directory")
        settings.portal_state_dir = Path(portal_dir)
    # A local provider endpoint is useful for deterministic protocol tests.
    if value.get("doubao_url"):
        settings.doubao_url = value["doubao_url"]
    return settings
