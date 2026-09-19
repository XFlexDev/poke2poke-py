from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
import secrets
import socket
import urllib.parse
from collections.abc import Awaitable, Callable
from functools import wraps
from pathlib import PurePath
from typing import Annotated, Literal, TypeVar

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_http_headers
from httpx import AsyncClient
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from .config import Settings
from .db import InvalidCursor, MessageStore, StoredMessage
from .poke_client import BackgroundTasks, PokeNotifier, PushResult

log = logging.getLogger(__name__)
T = TypeVar("T")


class SendResult(BaseModel):
    message_id: str
    recipient: str
    sender: str
    sent_at: str
    status: Literal["unread"] = "unread"
    notification: Literal["queued", "disabled"]
    attachments: list[str]
    next_step: str = (
        "The recipient will see this message via check_poke_messages and may acknowledge it; "
        "you will be notified in Poke when they do."
    )


class InboxMessage(BaseModel):
    message_id: str
    sender: str
    recipient: str
    message: str
    attachments: list[str]
    sent_at: str
    status: Literal["unread", "read"]
    read_at: str | None
    ack_note: str | None


class Inbox(BaseModel):
    you: str
    messages: list[InboxMessage]
    count: int
    has_more: bool
    next_cursor: str | None
    unread_total: int
    hint: str


class AckResult(BaseModel):
    message_id: str
    status: Literal["acknowledged", "already_acknowledged"]
    read_at: str
    sender: str
    notification: Literal["queued", "disabled", "not_sent"]
    note: str


class UploadResult(BaseModel):
    url: str
    filename: str
    content_type: str
    size_bytes: int
    next_step: str = "Pass `url` in attachment_urls of send_poke_message."


class RecipientsResult(BaseModel):
    you: str
    recipients: list[str]
    hint: str = "Use one of `recipients` as the `recipient` argument of send_poke_message."


def resolve_caller(settings: Settings) -> str:
    try:
        headers = get_http_headers()
    except Exception:
        headers = {}
    authorization = headers.get("authorization", "")
    if not authorization.startswith("Bearer "):
        raise ToolError(
            "Unauthorized: send the header `Authorization: Bearer <your poke2poke token>`."
        )
    token = authorization[7:]
    for handle, configured in settings.tokens.items():
        if configured and secrets.compare_digest(token, configured):
            return handle
    raise ToolError("Unauthorized: bearer token does not match any configured identity.")


def _guarded(label: str) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    def decorate(function: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(function)
        async def wrapped(*args: object, **kwargs: object) -> T:
            try:
                return await function(*args, **kwargs)
            except ToolError:
                raise
            except Exception:
                log.exception("tool failed: %s", label)
                raise ToolError(
                    f"Internal error while {label}; try again. If it persists contact the operator."
                ) from None

        return wrapped

    return decorate


def _validate_recipient(settings: Settings, caller: str, recipient: str) -> str:
    normalized = recipient.strip().lower()
    if normalized == caller:
        raise ToolError(f"You cannot send a message to yourself ('{caller}').")
    if normalized not in settings.tokens:
        others = sorted(handle for handle in settings.tokens if handle != caller)
        valid = ", ".join(others) or "(none)"
        raise ToolError(
            f"Unknown recipient '{normalized}'. Valid recipients for you: {valid}. "
            f"(You are '{caller}'.)"
        )
    return normalized


def _validate_attachments(settings: Settings, urls: list[str]) -> None:
    if len(urls) > settings.max_attachments:
        raise ToolError(
            f"attachment_urls contains {len(urls)} items; maximum is {settings.max_attachments}. "
            "Upload fewer files or send them in separate messages."
        )
    for index, url in enumerate(urls):
        parsed = urllib.parse.urlparse(url)
        host = (parsed.hostname or "").lower()
        if parsed.scheme.lower() != "https" or host not in settings.cdn_hosts:
            hosts = ", ".join(sorted(settings.cdn_hosts))
            raise ToolError(
                f"attachment_urls[{index}] must be an https:// URL on an allowed CDN host "
                f"({hosts}); got '{url}'."
            )


def _message_to_inbox(message: StoredMessage) -> InboxMessage:
    return InboxMessage(
        message_id=message.msg_id,
        sender=message.sender_handle,
        recipient=message.recipient_handle,
        message=message.text,
        attachments=message.attachments,
        sent_at=message.timestamp,
        status=message.status,
        read_at=message.read_at,
        ack_note=message.ack_note,
    )


async def _push_and_record(
    store: MessageStore,
    notifier: PokeNotifier,
    msg_id: str,
    recipient: str,
    message: str,
    sender: str,
    *,
    event: str = "message",
    note: str = "",
    attachments: list[dict[str, str]] | None = None,
) -> PushResult:
    result = await notifier.push(
        recipient, message, sender, event=event, note=note, attachments=attachments
    )
    await store.record_push(msg_id, result.status, result.attempts, result.error)
    return result


async def _push_ack_notification(
    notifier: PokeNotifier,
    msg_id: str,
    recipient: str,
    message: str,
    sender: str,
    note: str,
) -> None:
    result = await notifier.push(
        recipient,
        message,
        sender,
        event="acknowledgment",
        note=note,
    )
    log.info(
        "ack notification msg_id=%s to=%s status=%s attempts=%s error=%s",
        msg_id,
        recipient,
        result.status,
        result.attempts,
        result.error,
    )


def _is_private(host: str, addresses: list[str]) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        if ipaddress.ip_address(host).is_private:
            return True
    except ValueError:
        pass
    return any(
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        for address in (ipaddress.ip_address(value) for value in addresses)
    )


async def _guard_source_url(settings: Settings, url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise ToolError("source_url must be an https:// URL.")
    if settings.allow_private_upload_sources:
        return
    try:
        infos = await asyncio.to_thread(
            socket.getaddrinfo, parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM
        )
        addresses = list({str(info[4][0]) for info in infos})
    except OSError as exc:
        raise ToolError(
            f"Could not resolve source_url host '{parsed.hostname}'; check the URL and try again."
        ) from exc
    if _is_private(parsed.hostname, addresses):
        raise ToolError("source_url resolves to a private/internal address, which is not allowed.")


def _safe_filename(filename: str) -> str:
    value = PurePath(filename).name
    value = re.sub(r'[\x00-\x1f\x7f"\\]', "", value).strip()
    return value or "attachment"


def register(
    mcp: FastMCP,
    settings: Settings,
    store: MessageStore,
    notifier: PokeNotifier,
    background: BackgroundTasks,
) -> FastMCP:
    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=False, destructiveHint=False)
    )
    @_guarded("sending a Poke message")
    async def send_poke_message(
        recipient: Annotated[
            str,
            Field(
                description=(
                    "Handle of the recipient identity, e.g. 'aatu'. Must be one of the values "
                    "returned by list_poke_recipients (case-insensitive; cannot be your own "
                    "handle)."
                ),
                examples=["aatu"],
            ),
        ],
        message: Annotated[
            str,
            Field(
                default="",
                description=(
                    "Message text (plain UTF-8, newlines allowed). Optional only when "
                    "attachment_urls is provided. Maximum 4000 characters."
                ),
                examples=["Hello from usa"],
                max_length=4000,
            ),
        ] = "",
        attachment_urls: Annotated[
            list[str] | None,
            Field(
                default=None,
                description=(
                    "Optional list (max 10) of https:// URLs on the configured CDN, exactly as "
                    "returned in `url` by upload_poke_file."
                ),
                examples=[["https://cdn.example.com/file.txt"]],
                max_length=10,
            ),
        ] = None,
    ) -> SendResult:
        """Send a message to another Poke2Poke identity.

        You are identified by your bearer token (see `sender` in the result). The message is
        stored durably first, then a best-effort push notification is queued for the recipient's
        Poke agent. If the recipient has no Poke API key configured, `notification` is "disabled"
        but they will still see the message when they call check_poke_messages.

        Provide `message`, `attachment_urls`, or both. Attachment URLs must come from
        upload_poke_file. Returns an opaque `message_id`; the recipient acknowledges it later via
        ack_poke_message and you are notified when they do. Call list_poke_recipients if you are
        unsure which `recipient` handles exist — never guess a handle.
        """
        caller = resolve_caller(settings)
        recipient_name = _validate_recipient(settings, caller, recipient)
        urls = attachment_urls or []
        if not message.strip() and not urls:
            raise ToolError(
                "Provide a non-empty `message`, one or more `attachment_urls`, or both."
            )
        if len(message) > settings.max_message_chars:
            raise ToolError(
                f"`message` is {len(message)} characters; maximum is {settings.max_message_chars}. "
                "Split it into several messages."
            )
        _validate_attachments(settings, urls)
        stored = await store.insert_message(caller, recipient_name, message, urls)
        notification: Literal["queued", "disabled"] = (
            "queued" if settings.api_keys.get(recipient_name) else "disabled"
        )
        background.create(
            _push_and_record(
                store,
                notifier,
                stored.msg_id,
                recipient_name,
                message,
                caller,
                attachments=[{"url": url} for url in urls] or None,
            )
        )
        return SendResult(
            message_id=stored.msg_id,
            recipient=recipient_name,
            sender=caller,
            sent_at=stored.timestamp,
            notification=notification,
            attachments=urls,
        )

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, destructiveHint=False)
    )
    @_guarded("checking Poke messages")
    async def check_poke_messages(
        unread_only: Annotated[
            bool,
            Field(
                description=(
                    "true (default): only messages you have not acknowledged yet. false: include "
                    "acknowledged messages too."
                )
            ),
        ] = True,
        limit: Annotated[
            int, Field(description="Page size, 1-100 (default 50).", ge=1, le=100)
        ] = 50,
        cursor: Annotated[
            str | None,
            Field(
                description=(
                    "Opaque pagination cursor. Omit for the first page; for later pages pass the "
                    "`next_cursor` value from the previous response unchanged."
                )
            ),
        ] = None,
    ) -> Inbox:
        """List messages addressed to you (the identity behind your bearer token), oldest first.

        By default only unacknowledged messages (status "unread") are returned; set
        unread_only=false to include acknowledged ones. Results are paginated: when `has_more` is
        true, call again with `cursor` set to the returned `next_cursor` (an opaque string — pass
        it back unchanged, never construct one). `unread_total` is the total number of
        unacknowledged messages regardless of paging.

        After handling a message, call ack_poke_message with its exact `message_id` so the sender
        learns you have read it.
        """
        caller = resolve_caller(settings)
        try:
            messages, next_cursor = await store.list_messages(
                caller, unread_only=unread_only, limit=limit, cursor=cursor
            )
        except InvalidCursor as exc:
            raise ToolError(
                "`cursor` is not a valid pagination cursor. Pass the exact `next_cursor` value "
                "returned "
                "by a previous check_poke_messages call, or omit it to start from the beginning."
            ) from exc
        return Inbox(
            you=caller,
            messages=[_message_to_inbox(message) for message in messages],
            count=len(messages),
            has_more=next_cursor is not None,
            next_cursor=next_cursor,
            unread_total=await store.count_unread(caller),
            hint=(
                "Pass next_cursor as `cursor` to fetch the next page."
                if next_cursor
                else (
                    "Call ack_poke_message with a message_id once you have handled a message "
                    "so the sender knows."
                )
            ),
        )

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=True, destructiveHint=False)
    )
    @_guarded("acknowledging a Poke message")
    async def ack_poke_message(
        message_id: Annotated[
            str,
            Field(
                description=(
                    "Exact `message_id` value of a message returned by check_poke_messages "
                    "(opaque string; do not modify or guess)."
                ),
                min_length=1,
            ),
        ],
        note: Annotated[
            str,
            Field(
                default="",
                description=(
                    "Optional short reply (max 1000 characters) shown to the sender inside the "
                    "acknowledgement notification."
                ),
                max_length=1000,
            ),
        ] = "",
    ) -> AckResult:
        """Mark one of your received messages as read/acknowledged and notify its sender.

        Use the exact `message_id` from check_poke_messages. `note` is an optional short reply
        included in the notification to the sender. Acknowledging is idempotent: acknowledging an
        already-acknowledged message returns status "already_acknowledged" and does not notify the
        sender again. The notification is delivered in the background; `notification` is "queued",
        "disabled" (sender has no Poke API key) or "not_sent" (already acknowledged). Fails with a
        clear error if the message_id does not exist or is not addressed to you.
        """
        caller = resolve_caller(settings)
        message = await store.get_message(message_id, caller)
        result = await store.mark_read(message_id, caller, note.strip())
        if result == "not_found" or message is None:
            raise ToolError(
                f"No message with message_id '{message_id}' is addressed to you ('{caller}'). "
                "Call check_poke_messages to see your messages and copy the exact `message_id`."
            )
        if result == "already_acknowledged":
            return AckResult(
                message_id=message_id,
                status=result,
                read_at=message.read_at or message.timestamp,
                sender=message.sender_handle,
                notification="not_sent",
                note=message.ack_note or "",
            )
        updated = await store.get_message(message_id, caller)
        if updated is None:
            raise ToolError(
                "Internal error while reading the acknowledged message; try again. "
                "If it persists contact the operator."
            )
        notification: Literal["queued", "disabled"] = (
            "queued" if settings.api_keys.get(message.sender_handle) else "disabled"
        )
        background.create(
            _push_ack_notification(
                notifier,
                message_id,
                message.sender_handle,
                f"{caller} acknowledged your message"
                + (f": {note.strip()}" if note.strip() else ""),
                caller,
                note=note.strip(),
            )
        )
        return AckResult(
            message_id=message_id,
            status=result,
            read_at=updated.read_at or updated.timestamp,
            sender=message.sender_handle,
            notification=notification,
            note=note.strip(),
        )

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=False, destructiveHint=False)
    )
    @_guarded("uploading a Poke file")
    async def upload_poke_file(
        source_url: Annotated[
            str,
            Field(
                description=(
                    "Publicly reachable https:// URL of the file to copy. Private/internal hosts "
                    "are rejected."
                ),
                examples=["https://example.com/file.txt"],
            ),
        ],
        filename: Annotated[
            str,
            Field(
                default="attachment",
                description=(
                    "File name to store on the CDN, e.g. 'report.pdf'. Path components and control "
                    "characters are stripped; default 'attachment'."
                ),
                max_length=200,
            ),
        ] = "attachment",
    ) -> UploadResult:
        """Copy a publicly reachable HTTPS file to the CDN so it can be attached to a message.

        The file is downloaded server-side (redirects followed, private/internal hosts blocked,
        size limit enforced) and uploaded to the configured CDN. Returns `url`, which you pass in
        `attachment_urls` of send_poke_message. Only https:// source URLs are accepted.
        """
        resolve_caller(settings)
        current = source_url
        data = bytearray()
        content_type = "application/octet-stream"
        async with AsyncClient(follow_redirects=False, timeout=60) as client:
            for hop in range(6):
                await _guard_source_url(settings, current)
                async with client.stream("GET", current, follow_redirects=False) as response:
                    if response.is_redirect:
                        if hop == 5 or not response.headers.get("location"):
                            raise ToolError("Could not download source_url: too many redirects.")
                        current = urllib.parse.urljoin(current, response.headers["location"])
                        continue
                    if response.status_code >= 400:
                        raise ToolError(
                            f"Could not download source_url: HTTP {response.status_code}"
                        )
                    header_size = response.headers.get("content-length")
                    if header_size and int(header_size) > settings.max_upload_bytes:
                        raise ToolError(
                            f"File is larger than the "
                            f"{settings.max_upload_bytes // (1024 * 1024)} MiB limit"
                        )
                    content_type = response.headers.get(
                        "content-type", "application/octet-stream"
                    ).split(";", 1)[0]
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > settings.max_upload_bytes:
                            raise ToolError(
                                f"File is larger than the "
                                f"{settings.max_upload_bytes // (1024 * 1024)} MiB limit"
                            )
                    break
            else:
                raise ToolError("Could not download source_url: too many redirects.")
        safe_name = _safe_filename(filename)
        async with AsyncClient(timeout=120) as client:
            response = await client.post(
                settings.cdn_upload_url,
                files={"file": (safe_name, bytes(data), content_type)},
            )
        if response.status_code >= 400:
            raise ToolError(f"Could not upload file: HTTP {response.status_code}")
        try:
            result = response.json()
            returned_url = result.get("url") or result.get("cdn_url") or result.get("fileUrl")
        except (ValueError, AttributeError) as exc:
            raise ToolError(
                "CDN upload returned invalid JSON; check the CDN configuration."
            ) from exc
        parsed = urllib.parse.urlparse(str(returned_url or ""))
        if parsed.scheme != "https" or (parsed.hostname or "").lower() not in settings.cdn_hosts:
            raise ToolError("CDN upload did not return a valid HTTPS URL on an allowed CDN host.")
        return UploadResult(
            url=str(returned_url),
            filename=safe_name,
            content_type=content_type,
            size_bytes=len(data),
        )

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, destructiveHint=False)
    )
    @_guarded("listing Poke recipients")
    async def list_poke_recipients() -> RecipientsResult:
        """Show who you are and which handles you can send messages to.

        Returns `you` (your handle, derived from your bearer token) and `recipients` (all other
        configured handles). Call this before send_poke_message if you are unsure of a handle —
        never guess.
        """
        caller = resolve_caller(settings)
        return RecipientsResult(
            you=caller, recipients=sorted(handle for handle in settings.tokens if handle != caller)
        )

    return mcp
