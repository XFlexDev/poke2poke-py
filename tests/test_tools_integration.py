from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import uvicorn
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

from poke2poke import tools
from poke2poke.app import create_app
from poke2poke.config import Settings
from poke2poke.db import MessageStore
from poke2poke.poke_client import PokeNotifier


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _settings(path: Path) -> Settings:
    return Settings(
        port=_free_port(),
        db_path=str(path),
        poke_inbound_url="https://poke.example/inbound",
        cdn_upload_url="https://cdn.example/upload",
        cdn_hosts=frozenset({"cdn.example"}),
        tokens={"usa": "usa-token", "aatu": "aatu-token"},
        api_keys={"usa": "usa-api"},
        allow_private_upload_sources=True,
    )


async def _wait_for(predicate, *, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0.01)


@pytest.fixture
async def service(tmp_path: Path) -> AsyncIterator[dict[str, object]]:
    requests: list[httpx.Request] = []

    async def recorder(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, request=request, json={"ok": True})

    settings = _settings(tmp_path / "messages.sqlite")
    notifier_client = httpx.AsyncClient(transport=httpx.MockTransport(recorder))
    notifier = PokeNotifier(settings, notifier_client)
    app = create_app(settings, notifier=notifier)
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            app.http_app(path="/mcp"),
            host="127.0.0.1",
            port=port,
            log_level="warning",
        )
    )
    task = asyncio.create_task(server.serve())
    try:
        await _wait_for(lambda: server.started)
        yield {
            "settings": settings,
            "requests": requests,
            "url": f"http://127.0.0.1:{port}/mcp",
        }
    finally:
        server.should_exit = True
        await task
        await notifier_client.aclose()


def _client(url: str, token: str) -> Client:
    return Client(StreamableHttpTransport(url, headers={"Authorization": f"Bearer {token}"}))


async def _data(client: Client, name: str, arguments: dict[str, object] | None = None) -> dict:
    result = await client.call_tool(name, arguments or {})
    structured = result.structured_content
    assert isinstance(structured, dict)
    return structured


def _json_body(request: httpx.Request) -> dict:
    return httpx.Response(200, request=request, content=request.content).json()


async def test_tools_list_has_contract_metadata(service: dict[str, object]) -> None:
    async with _client(str(service["url"]), "usa-token") as client:
        tools = await client.list_tools()
    assert len(tools) == 5
    for tool in tools:
        assert tool.description
        assert tool.outputSchema
        for schema in tool.inputSchema.get("properties", {}).values():
            assert schema.get("description")


async def test_authentication_errors(service: dict[str, object]) -> None:
    transport = StreamableHttpTransport(str(service["url"]))
    with pytest.raises(Exception, match="Authorization: Bearer"):
        async with Client(transport) as client:
            await client.call_tool("list_poke_recipients", {})
    with pytest.raises(Exception, match="does not match"):
        async with _client(str(service["url"]), "wrong") as client:
            await client.call_tool("list_poke_recipients", {})


async def test_send_and_notification_delivery(service: dict[str, object]) -> None:
    async with _client(str(service["url"]), "usa-token") as usa:
        first = await _data(usa, "send_poke_message", {"recipient": "aatu", "message": "hello"})
    assert first["message_id"] and len(first["message_id"]) > 10
    assert first["sender"] == "usa"
    assert first["recipient"] == "aatu"
    assert first["notification"] == "disabled"

    async with _client(str(service["url"]), "aatu-token") as aatu:
        second = await _data(aatu, "send_poke_message", {"recipient": "usa", "message": "reply"})
    assert second["notification"] == "queued"
    requests = service["requests"]
    assert isinstance(requests, list)
    await _wait_for(lambda: len(requests) == 1)
    request = requests[0]
    assert request.headers["Authorization"] == "Bearer usa-api"
    body = _json_body(request)
    assert set(body) == {"message", "text", "sender", "actor", "event", "type", "status"}
    assert body["event"] == "message"
    store = MessageStore(str(service["settings"].db_path))
    row = await store.get_message(second["message_id"], "usa")
    assert row and row.push_status == "delivered" and row.push_attempts == 1


async def test_send_validation_and_normalization(service: dict[str, object]) -> None:
    async with _client(str(service["url"]), "usa-token") as client:
        with pytest.raises(Exception, match="Valid recipients for you: aatu"):
            await client.call_tool("send_poke_message", {"recipient": "other", "message": "x"})
        with pytest.raises(Exception, match="yourself"):
            await client.call_tool("send_poke_message", {"recipient": "usa", "message": "x"})
        with pytest.raises(Exception, match="non-empty"):
            await client.call_tool("send_poke_message", {"recipient": "aatu"})
        with pytest.raises(Exception, match="message"):
            await client.call_tool(
                "send_poke_message", {"recipient": "aatu", "message": "x" * 4001}
            )
        with pytest.raises(Exception, match="allowed CDN host"):
            await client.call_tool(
                "send_poke_message",
                {"recipient": "aatu", "message": "x", "attachment_urls": ["https://bad/x"]},
            )
        result = await _data(
            client, "send_poke_message", {"recipient": "  AATU ", "message": "normalized"}
        )
    assert result["recipient"] == "aatu"


async def test_inbox_isolated_and_ack_is_idempotent(service: dict[str, object]) -> None:
    async with _client(str(service["url"]), "usa-token") as usa:
        sent = await _data(usa, "send_poke_message", {"recipient": "aatu", "message": "hello"})
    async with _client(str(service["url"]), "aatu-token") as aatu:
        inbox = await _data(aatu, "check_poke_messages")
    assert inbox["you"] == "aatu"
    assert inbox["count"] == 1 and inbox["unread_total"] == 1
    message = inbox["messages"][0]
    assert set(message) == {
        "message_id",
        "sender",
        "recipient",
        "message",
        "attachments",
        "sent_at",
        "status",
        "read_at",
        "ack_note",
    }
    assert message["status"] == "unread"
    async with _client(str(service["url"]), "usa-token") as usa:
        own_inbox = await _data(usa, "check_poke_messages")
    assert all(item["message_id"] != sent["message_id"] for item in own_inbox["messages"])

    requests = service["requests"]
    assert isinstance(requests, list)
    async with _client(str(service["url"]), "aatu-token") as aatu:
        ack = await _data(
            aatu,
            "ack_poke_message",
            {"message_id": sent["message_id"], "note": "handled"},
        )
        assert ack["status"] == "acknowledged"
        assert ack["notification"] == "queued"
        await _wait_for(lambda: len(requests) == 1)
        body = _json_body(requests[0])
        assert body["event"] == "acknowledgment"
        assert body["note"] == "handled"
        second = await _data(aatu, "ack_poke_message", {"message_id": sent["message_id"]})
    assert second["status"] == "already_acknowledged"
    assert second["notification"] == "not_sent"
    assert len(requests) == 1
    store = MessageStore(str(service["settings"].db_path))
    row = await store.get_message(sent["message_id"], "aatu")
    assert row and row.push_status == "skipped"
    with pytest.raises(Exception, match="check_poke_messages"):
        async with _client(str(service["url"]), "aatu-token") as aatu:
            await aatu.call_tool("ack_poke_message", {"message_id": "missing"})
    async with _client(str(service["url"]), "aatu-token") as aatu:
        read_inbox = await _data(aatu, "check_poke_messages", {"unread_only": False})
    assert read_inbox["messages"][0]["status"] == "read"
    assert read_inbox["messages"][0]["read_at"]
    assert read_inbox["messages"][0]["ack_note"] == "handled"


async def test_http_pagination_and_recipients(service: dict[str, object]) -> None:
    async with _client(str(service["url"]), "usa-token") as usa:
        for index in range(5):
            await _data(
                usa,
                "send_poke_message",
                {"recipient": "aatu", "message": f"message {index}"},
            )
    ids: list[str] = []
    cursor = None
    counts = []
    async with _client(str(service["url"]), "aatu-token") as aatu:
        while True:
            args: dict[str, object] = {"limit": 2}
            if cursor:
                args["cursor"] = cursor
            page = await _data(aatu, "check_poke_messages", args)
            counts.append(page["count"])
            ids.extend(item["message_id"] for item in page["messages"])
            cursor = page["next_cursor"]
            if not page["has_more"]:
                break
        recipients = await _data(aatu, "list_poke_recipients")
        assert recipients == {
            "you": "aatu",
            "recipients": ["usa"],
            "hint": "Use one of `recipients` as the `recipient` argument of send_poke_message.",
        }
        with pytest.raises(Exception, match="next_cursor"):
            await aatu.call_tool("check_poke_messages", {"cursor": "garbage"})
    assert counts == [2, 2, 1]
    assert len(ids) == len(set(ids)) == 5


async def test_health(service: dict[str, object]) -> None:
    async with httpx.AsyncClient() as client:
        response = await client.get(str(service["url"]).removesuffix("/mcp") + "/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "poke2poke", "version": "0.2.0"}


async def test_upload_validation_redirect_and_multipart(
    service: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    upload_requests: list[httpx.Request] = []

    async def upload_recorder(request: httpx.Request) -> httpx.Response:
        upload_requests.append(request)
        return httpx.Response(
            200,
            request=request,
            json={"url": "https://cdn.example/file.txt"},
        )

    async def source_recorder(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/big":
            return httpx.Response(
                200,
                request=request,
                headers={"content-length": "200000000"},
                content=b"x",
            )
        if request.url.path == "/redirect":
            return httpx.Response(
                302,
                request=request,
                headers={"location": "https://source.example/file.txt"},
            )
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/plain"},
            content=b"hello",
        )

    def client_factory(*args, **kwargs):
        transport = httpx.MockTransport(
            lambda request: (
                upload_recorder(request) if request.method == "POST" else source_recorder(request)
            )
        )
        return httpx.AsyncClient(*args, transport=transport, **kwargs)

    monkeypatch.setattr(tools, "AsyncClient", client_factory)
    async with _client(str(service["url"]), "usa-token") as client:
        with pytest.raises(Exception, match="https://"):
            await client.call_tool(
                "upload_poke_file", {"source_url": "http://source.example/file.txt"}
            )
        with pytest.raises(Exception, match="MiB limit"):
            await client.call_tool("upload_poke_file", {"source_url": "https://source.example/big"})
        redirected = await _data(
            client, "upload_poke_file", {"source_url": "https://source.example/redirect"}
        )
        uploaded = await _data(
            client,
            "upload_poke_file",
            {"source_url": "https://source.example/file.txt", "filename": '../evil"name.txt'},
        )
    assert redirected["url"] == "https://cdn.example/file.txt"
    assert uploaded["url"] == "https://cdn.example/file.txt"
    assert uploaded["size_bytes"] == 5
    assert uploaded["content_type"] == "text/plain"
    assert uploaded["filename"] == "evilname.txt"
    assert len(upload_requests) == 2
    assert b'name="file"' in upload_requests[-1].content
