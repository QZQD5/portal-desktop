import json
import os

import pytest

from gateway.__main__ import validate_setup
from conftest import unit_settings as Settings
from gateway.instance import Lease, alive, read_lease


def test_lease_conflict_stale_recovery_and_identity_guard(tmp_path):
    first = Lease(tmp_path).acquire()
    try:
        assert alive(os.getpid())
        with pytest.raises((OSError, RuntimeError)):
            Lease(tmp_path).acquire()
        assert first.update(state="ready", origin="http://127.0.0.1:12345")
        assert read_lease(tmp_path)["instanceId"] == first.record["instanceId"]
    finally:
        first.close()
    assert read_lease(tmp_path) is None
    (tmp_path / "gateway.lock").write_text(
        json.dumps({"schema": "qwaudio.gateway-lock/v1", "instanceId": "stale", "pid": -1})
    )
    recovered = Lease(tmp_path).acquire()
    assert recovered.record["instanceId"] != "stale"
    recovered.close()


def test_setup_fails_before_taking_lease(tmp_path, monkeypatch):
    monkeypatch.delenv("AUDIO_ALLOW_UNCONFIGURED", raising=False)
    with pytest.raises(ValueError, match="provider URL"):
        validate_setup(Settings(doubao_url="", data_dir=tmp_path))
    assert not (tmp_path / "gateway.lock").exists()
