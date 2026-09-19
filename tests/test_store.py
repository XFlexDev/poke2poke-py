from __future__ import annotations

import sqlite3

import pytest

from poke2poke.db import InvalidCursor, MessageStore


async def test_store_pagination_and_ack(tmp_path) -> None:
    store = MessageStore(str(tmp_path / "messages.sqlite"))
    store.initialize()
    for index in range(7):
        await store.insert_message("usa", "aatu", f"message {index}", [])
    first, cursor = await store.list_messages("aatu", unread_only=True, limit=3, cursor=None)
    second, cursor2 = await store.list_messages("aatu", unread_only=True, limit=3, cursor=cursor)
    third, cursor3 = await store.list_messages("aatu", unread_only=True, limit=3, cursor=cursor2)
    messages = first + second + third
    assert len(messages) == 7
    assert {item.text for item in messages} == {f"message {i}" for i in range(7)}
    assert cursor3 is None
    assert await store.count_unread("aatu") == 7
    assert await store.mark_read(first[0].msg_id, "aatu", "done") == "acknowledged"
    assert await store.mark_read(first[0].msg_id, "aatu", "again") == "already_acknowledged"
    assert await store.mark_read(first[0].msg_id, "usa", "") == "not_found"
    await store.record_push(first[0].msg_id, "delivered", 1, None)
    message = await store.get_message(first[0].msg_id, "aatu")
    assert message and message.push_status == "delivered"
    with pytest.raises(InvalidCursor):
        await store.list_messages("aatu", unread_only=True, limit=3, cursor="bad")


async def test_legacy_migration_and_isolation(tmp_path) -> None:
    path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE messages (msg_id TEXT PRIMARY KEY,sender_handle TEXT,"
            "recipient_handle TEXT,"
            "text TEXT,metadata TEXT,timestamp TEXT,status TEXT)"
        )
        connection.execute(
            "INSERT INTO messages VALUES (?,?,?,?,?,?,?)",
            (
                "old",
                "usa",
                "aatu",
                "old text",
                '{"attachments":["https://cdn/x"]}',
                "2024",
                "unread",
            ),
        )
    store = MessageStore(str(path))
    store.initialize()
    rows, _ = await store.list_messages("aatu", unread_only=False, limit=10, cursor=None)
    assert rows[0].msg_id == "old"
    assert rows[0].attachments == ["https://cdn/x"]
    assert await store.list_messages("usa", unread_only=False, limit=10, cursor=None) == ([], None)
