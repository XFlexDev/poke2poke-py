from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from . import __version__
from .config import Settings, load_settings
from .db import MessageStore
from .poke_client import BackgroundTasks, PokeNotifier
from .tools import register

log = logging.getLogger(__name__)
INSTRUCTIONS = (
    "Poke2Poke is a message inbox shared by a few named identities (handles). Authenticate every "
    "request with `Authorization: Bearer <token>`; the token decides which handle you are. Flow: "
    "list_poke_recipients (discover handles) -> send_poke_message (returns an opaque message_id) "
    "-> the recipient calls check_poke_messages (paginated with next_cursor) -> ack_poke_message "
    "(idempotent; notifies the sender). Messages are persisted before any notification; Poke push "
    "notifications are best-effort. Attach files by first calling upload_poke_file. All errors say "
    "what was wrong and what to do next."
)


def create_app(
    settings: Settings | None = None,
    notifier: PokeNotifier | None = None,
) -> FastMCP:
    resolved = settings or load_settings()
    store = MessageStore(resolved.db_path)
    resolved_notifier = notifier or PokeNotifier(resolved)
    background = BackgroundTasks()

    @contextlib.asynccontextmanager
    async def lifespan(server: FastMCP) -> AsyncIterator[None]:
        del server
        await asyncio.to_thread(store.initialize)
        try:
            yield
        finally:
            await background.drain()
            await resolved_notifier.close()

    mcp = FastMCP(
        "poke2poke",
        instructions=INSTRUCTIONS,
        version=__version__,
        lifespan=lifespan,
    )
    register(mcp, resolved, store, resolved_notifier, background)

    @mcp.custom_route("/health", methods=["GET"])
    async def health(request: Request) -> JSONResponse:
        del request
        try:
            await store.check()
        except Exception:
            return JSONResponse(
                {"status": "unavailable", "service": "poke2poke", "version": __version__},
                status_code=503,
            )
        return JSONResponse({"status": "ok", "service": "poke2poke", "version": __version__})

    return mcp


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("POKE2POKE_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = load_settings()
    create_app(settings).run(
        transport="http",
        host=os.environ.get("POKE2POKE_HOST", "0.0.0.0"),
        port=settings.port,
        path="/mcp",
    )
