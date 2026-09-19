from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Literal

import httpx

from .config import Settings

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PushResult:
    status: Literal["delivered", "failed", "skipped"]
    attempts: int
    http_status: int | None
    error: str | None


class PokeNotifier:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None):
        self.settings = settings
        self.client = client or httpx.AsyncClient()
        self._owns_client = client is None

    async def push(
        self,
        recipient: str,
        message: str,
        sender: str,
        *,
        event: str = "message",
        note: str = "",
        status: str = "",
        attachments: list[dict[str, str]] | None = None,
    ) -> PushResult:
        key = self.settings.api_keys.get(recipient)
        if not key:
            log.info("push skipped recipient=%s event=%s", recipient, event)
            return PushResult("skipped", 0, None, None)
        payload: dict[str, object] = {
            "message": message,
            "text": message,
            "sender": sender,
            "actor": sender,
            "event": event,
            "type": event,
            "status": status or ("acknowledged" if event == "acknowledgment" else "sent"),
        }
        if note:
            payload["note"] = note
        if attachments:
            payload["attachments"] = attachments
        attempts = max(1, self.settings.poke_push_attempts)
        last_status: int | None = None
        last_error: str | None = None
        for attempt in range(1, attempts + 1):
            try:
                response = await self.client.post(
                    self.settings.poke_inbound_url,
                    json=payload,
                    headers={"Authorization": f"Bearer {key}"},
                    timeout=self.settings.poke_push_timeout_s,
                )
                last_status = response.status_code
                log.info(
                    "poke push attempt recipient=%s event=%s status_code=%s attempt=%s",
                    recipient,
                    event,
                    response.status_code,
                    attempt,
                )
                log.debug("poke push response recipient=%s body=%s", recipient, response.text[:500])
                if 200 <= response.status_code < 300:
                    return PushResult("delivered", attempt, response.status_code, None)
                last_error = f"HTTP {response.status_code}"
                retryable = response.status_code == 429 or response.status_code >= 500
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_error = str(exc) or exc.__class__.__name__
                retryable = True
                log.info(
                    "poke push attempt recipient=%s event=%s status_code=%s attempt=%s",
                    recipient,
                    event,
                    None,
                    attempt,
                )
            if not retryable or attempt == attempts:
                break
            await asyncio.sleep(min(8.0, 0.5 * 2 ** (attempt - 1)) + random.uniform(0, 0.25))
        return PushResult(
            "failed", attempts if last_status is None else attempt, last_status, last_error
        )

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()


class BackgroundTasks:
    def __init__(self):
        self._tasks: set[asyncio.Task[object]] = set()

    def create(self, coroutine: Coroutine[object, object, object]) -> asyncio.Task[object]:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._done)
        return task

    def _done(self, task: asyncio.Task[object]) -> None:
        self._tasks.discard(task)
        if not task.cancelled():
            with contextlib.suppress(asyncio.CancelledError):
                error = task.exception()
                if error:
                    log.error(
                        "background task failed",
                        exc_info=(type(error), error, error.__traceback__),
                    )

    async def drain(self, timeout: float = 10) -> None:
        if not self._tasks:
            return
        _, pending = await asyncio.wait(self._tasks, timeout=timeout)
        for task in pending:
            task.cancel()
