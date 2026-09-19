from __future__ import annotations

import json
import logging

import pytest

from poke2poke.config import ConfigError, load_settings


def test_env_only_config(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("POKE_INBOUND_URL", "https://poke.example/inbound")
    monkeypatch.setenv("CDN1_UPLOAD_URL", "https://cdn.example/upload")
    monkeypatch.setenv("POKE2POKE_CDN_HOSTS", "CDN.EXAMPLE, files.example")
    monkeypatch.setenv("POKE2POKE_KEY_USA", " token ")
    monkeypatch.setenv("POKE_API_KEY_USA", "key")
    settings = load_settings(tmp_path / "missing.json")
    assert settings.tokens == {"usa": "token"}
    assert settings.cdn_hosts == frozenset({"cdn.example", "files.example"})
    assert settings.port == 8765


def test_missing_required_values_lists_all_problems(tmp_path) -> None:
    with pytest.raises(ConfigError) as raised:
        load_settings(tmp_path / "missing.json")
    message = str(raised.value)
    assert "poke_inbound_url" in message
    assert "cdn_upload_url" in message
    assert "cdn_hosts" in message
    assert "at least one token" in message


def test_file_values_and_env_override(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "port": 7000,
                "poke_inbound_url": "https://poke.example",
                "cdn_upload_url": "https://cdn.example",
                "cdn_hosts": ["cdn.example"],
                "tokens": {"USA": "old"},
                "api_keys": {"USA": "key"},
            }
        )
    )
    monkeypatch.setenv("POKE2POKE_PORT", "7001")
    monkeypatch.setenv("POKE2POKE_KEY_USA", "new")
    settings = load_settings(path)
    assert settings.port == 7001
    assert settings.tokens == {"usa": "new"}


def test_warns_for_api_keyless_handle(tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "poke_inbound_url": "https://poke.example",
                "cdn_upload_url": "https://cdn.example",
                "cdn_hosts": ["cdn.example"],
                "tokens": {"usa": "token"},
            }
        )
    )
    with caplog.at_level(logging.WARNING):
        load_settings(path)
    assert "push notifications disabled for handle usa" in caplog.text
