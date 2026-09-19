from __future__ import annotations

import socket

import pytest
from fastmcp.exceptions import ToolError

from poke2poke.config import Settings
from poke2poke.tools import _guard_source_url


def _settings(allow_private: bool = False) -> Settings:
    return Settings(
        port=8765,
        db_path=":memory:",
        poke_inbound_url="https://poke.example/inbound",
        cdn_upload_url="https://cdn.example/upload",
        cdn_hosts=frozenset({"cdn.example"}),
        tokens={"usa": "token"},
        api_keys={},
        allow_private_upload_sources=allow_private,
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1/x",
        "https://localhost/x",
        "https://10.0.0.1/x",
        "https://169.254.169.254/x",
    ],
)
async def test_guard_rejects_private_literals(url: str) -> None:
    with pytest.raises(ToolError, match="private/internal"):
        await _guard_source_url(_settings(), url)


async def test_guard_rejects_http() -> None:
    with pytest.raises(ToolError, match="https://"):
        await _guard_source_url(_settings(allow_private=True), "http://example.com/x")


async def test_guard_checks_hostname_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    def private(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", private)
    with pytest.raises(ToolError, match="private/internal"):
        await _guard_source_url(_settings(), "https://source.example/x")

    def public(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", public)
    await _guard_source_url(_settings(), "https://source.example/x")
