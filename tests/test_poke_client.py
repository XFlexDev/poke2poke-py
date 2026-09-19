from __future__ import annotations

import asyncio

import httpx
import pytest

from poke2poke.config import Settings
from poke2poke.poke_client import PokeNotifier


def make_settings(attempts: int = 4, api_keys: dict[str, str] | None = None) -> Settings:
    return Settings(
        port=8765,
        db_path=":memory:",
        poke_inbound_url="https://poke.example/inbound",
        cdn_upload_url="https://cdn.example/upload",
        cdn_hosts=frozenset({"cdn.example"}),
        tokens={"usa": "token"},
        api_keys={"aatu": "key"} if api_keys is None else api_keys,
        poke_push_attempts=attempts,
    )


async def test_push_payload_and_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [httpx.Response(503), httpx.Response(503), httpx.Response(200)]
    transport = httpx.MockTransport(lambda request: responses.pop(0))
    notifier = PokeNotifier(make_settings(), httpx.AsyncClient(transport=transport))

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    result = await notifier.push(
        "aatu", "hello", "usa", note="n", attachments=[{"url": "https://cdn.example/x"}]
    )
    assert result.status == "delivered"
    assert result.attempts == 3
    await notifier.client.aclose()


async def test_push_skips_without_key() -> None:
    notifier = PokeNotifier(make_settings(api_keys={}))
    assert (await notifier.push("aatu", "hello", "usa")).status == "skipped"
    await notifier.close()


async def test_push_non_retryable_failure_reports_attempt_count() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(400, request=request))
    notifier = PokeNotifier(make_settings(), httpx.AsyncClient(transport=transport))
    result = await notifier.push("aatu", "hello", "usa")
    assert result.status == "failed"
    assert result.attempts == 1
    assert result.http_status == 400
    await notifier.client.aclose()
