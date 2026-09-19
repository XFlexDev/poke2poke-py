from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import json
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal


class InvalidCursor(ValueError):
    """Raised when a pagination cursor cannot be decoded."""


@dataclass
class StoredMessage:
    msg_id: str
    sender_handle: str
    recipient_handle: str
    text: str
    attachments: list[str]
    timestamp: str
    status: Literal["unread", "read"]
    read_at: str | None = None
    ack_note: str | None = None
    push_status: str | None = None
    push_attempts: int = 0
    push_error: str | None = None
    push_updated_at: str | None = None
    row_id: int = 0


def _encode_cursor(timestamp: str, row_id: int) -> str:
    raw = f"{timestamp}\x1f{row_id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[str, int]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        timestamp, row_id = raw.split("\x1f", 1)
        if not timestamp:
            raise ValueError
        return timestamp, int(row_id)
    except (ValueError, TypeError, UnicodeDecodeError, binascii.Error) as exc:
        raise InvalidCursor("invalid pagination cursor") from exc


class MessageStore:
    def __init__(self, path: str):
        self.path = path
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def initialize(self) -> None:
        if self._initialized:
            return
        with contextlib.closing(self._connect()) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS messages ("
                "msg_id TEXT PRIMARY KEY, sender_handle TEXT, recipient_handle TEXT, "
                "text TEXT, metadata TEXT, timestamp TEXT, status TEXT)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_recipient_status_time "
                "ON messages(recipient_handle,status,timestamp)"
            )
            existing = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(messages)").fetchall()
            }
            migrations = {
                "read_at": "TEXT",
                "ack_note": "TEXT",
                "push_status": "TEXT",
                "push_attempts": "INTEGER NOT NULL DEFAULT 0",
                "push_error": "TEXT",
                "push_updated_at": "TEXT",
            }
            for name, definition in migrations.items():
                if name not in existing:
                    connection.execute(f"ALTER TABLE messages ADD COLUMN {name} {definition}")
            connection.execute("DROP INDEX IF EXISTS idx_messages_recipient_time_id")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_recipient_time "
                "ON messages(recipient_handle,timestamp)"
            )
        self._initialized = True

    @staticmethod
    def _message(row: sqlite3.Row) -> StoredMessage:
        columns = set(row.keys())
        try:
            metadata = json.loads(row["metadata"] or "{}")
            attachments_value = (
                metadata.get("attachments", []) if isinstance(metadata, dict) else []
            )
            attachments = (
                [str(item) for item in attachments_value if isinstance(item, str)]
                if isinstance(attachments_value, list)
                else []
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            attachments = []
        return StoredMessage(
            msg_id=str(row["msg_id"]),
            sender_handle=str(row["sender_handle"]),
            recipient_handle=str(row["recipient_handle"]),
            text=str(row["text"] or ""),
            attachments=attachments,
            timestamp=str(row["timestamp"]),
            status="read" if row["status"] == "read" else "unread",
            read_at=row["read_at"] if "read_at" in columns else None,
            ack_note=row["ack_note"] if "ack_note" in columns else None,
            push_status=row["push_status"] if "push_status" in columns else None,
            push_attempts=int(row["push_attempts"] or 0) if "push_attempts" in columns else 0,
            push_error=row["push_error"] if "push_error" in columns else None,
            push_updated_at=row["push_updated_at"] if "push_updated_at" in columns else None,
            row_id=int(row["row_id"]) if "row_id" in columns else 0,
        )

    def _insert(
        self, sender: str, recipient: str, text: str, attachments: list[str]
    ) -> StoredMessage:
        msg_id = secrets.token_urlsafe(16)
        timestamp = datetime.now(UTC).isoformat(timespec="milliseconds")
        metadata = json.dumps({"attachments": attachments}, separators=(",", ":"))
        with contextlib.closing(self._connect()) as connection:
            connection.execute(
                "INSERT INTO messages "
                "(msg_id,sender_handle,recipient_handle,text,metadata,timestamp,status) "
                "VALUES (?,?,?,?,?,?,?)",
                (msg_id, sender, recipient, text, metadata, timestamp, "unread"),
            )
            row = connection.execute(
                "SELECT rowid AS row_id, * FROM messages WHERE msg_id=?", (msg_id,)
            ).fetchone()
        if row is None:
            raise RuntimeError("inserted message could not be read")
        return self._message(row)

    async def insert_message(
        self, sender: str, recipient: str, text: str, attachments: list[str]
    ) -> StoredMessage:
        return await asyncio.to_thread(self._insert, sender, recipient, text, attachments)

    def _list(
        self, recipient: str, unread_only: bool, limit: int, cursor: str | None
    ) -> tuple[list[StoredMessage], str | None]:
        query = "SELECT rowid AS row_id, * FROM messages WHERE recipient_handle=?"
        args: list[object] = [recipient]
        if unread_only:
            query += " AND status != 'read'"
        if cursor:
            timestamp, row_id = _decode_cursor(cursor)
            query += " AND (timestamp > ? OR (timestamp = ? AND rowid > ?))"
            args.extend([timestamp, timestamp, row_id])
        query += " ORDER BY timestamp ASC, rowid ASC LIMIT ?"
        args.append(limit + 1)
        with contextlib.closing(self._connect()) as connection:
            rows = connection.execute(query, args).fetchall()
        messages = [self._message(row) for row in rows[:limit]]
        next_cursor = (
            _encode_cursor(messages[-1].timestamp, messages[-1].row_id)
            if len(rows) > limit
            else None
        )
        return messages, next_cursor

    async def list_messages(
        self, recipient: str, *, unread_only: bool, limit: int, cursor: str | None
    ) -> tuple[list[StoredMessage], str | None]:
        return await asyncio.to_thread(self._list, recipient, unread_only, limit, cursor)

    def _count(self, recipient: str) -> int:
        with contextlib.closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM messages WHERE recipient_handle=? AND status != 'read'",
                (recipient,),
            ).fetchone()
        return int(row[0]) if row else 0

    async def count_unread(self, recipient: str) -> int:
        return await asyncio.to_thread(self._count, recipient)

    def _get(self, msg_id: str, recipient: str) -> StoredMessage | None:
        with contextlib.closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT rowid AS row_id, * FROM messages WHERE msg_id=? AND recipient_handle=?",
                (msg_id, recipient),
            ).fetchone()
        return self._message(row) if row else None

    async def get_message(self, msg_id: str, recipient: str) -> StoredMessage | None:
        return await asyncio.to_thread(self._get, msg_id, recipient)

    def _mark(
        self, msg_id: str, recipient: str, note: str
    ) -> Literal["acknowledged", "already_acknowledged", "not_found"]:
        read_at = datetime.now(UTC).isoformat(timespec="milliseconds")
        with contextlib.closing(self._connect()) as connection:
            cursor = connection.execute(
                "UPDATE messages SET status='read', read_at=?, ack_note=? "
                "WHERE msg_id=? AND recipient_handle=? AND status!='read'",
                (read_at, note, msg_id, recipient),
            )
            if cursor.rowcount == 1:
                return "acknowledged"
            row = connection.execute(
                "SELECT 1 FROM messages WHERE msg_id=? AND recipient_handle=?", (msg_id, recipient)
            ).fetchone()
        return "already_acknowledged" if row else "not_found"

    async def mark_read(
        self, msg_id: str, recipient: str, note: str
    ) -> Literal["acknowledged", "already_acknowledged", "not_found"]:
        return await asyncio.to_thread(self._mark, msg_id, recipient, note)

    def _record_push(
        self,
        msg_id: str,
        status: Literal["delivered", "failed", "skipped"],
        attempts: int,
        error: str | None,
    ) -> None:
        updated = datetime.now(UTC).isoformat(timespec="milliseconds")
        with contextlib.closing(self._connect()) as connection:
            connection.execute(
                "UPDATE messages SET push_status=?,push_attempts=?,push_error=?,push_updated_at=? "
                "WHERE msg_id=?",
                (status, attempts, error, updated, msg_id),
            )

    async def record_push(
        self,
        msg_id: str,
        status: Literal["delivered", "failed", "skipped"],
        attempts: int,
        error: str | None,
    ) -> None:
        await asyncio.to_thread(self._record_push, msg_id, status, attempts, error)

    async def check(self) -> None:
        await asyncio.to_thread(self._check)

    def _check(self) -> None:
        with contextlib.closing(self._connect()) as connection:
            connection.execute("SELECT 1").fetchone()
