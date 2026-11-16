import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

# The repository keeps config.json out of source control; provide test-only defaults.
_config_path = Path(__file__).with_name("config.json")
if not _config_path.exists():
    _config_path.write_text(json.dumps({
        "port": 8765, "db_path": "data.sqlite",
        "poke_inbound_url": "https://example.invalid/poke/inbound",
        "cdn_upload_url": "https://example.invalid/upload",
        "cdn_hosts": ["cdn.example.com"],
        "tokens": {"owner": "owner-token", "peer-full": "peer-token"},
        "api_keys": {"owner": "owner-api", "peer-full": "peer-api"},
        "trusted_peers": {}
    }))

import db
import tools


class FakeMCP:
    def __init__(self):
        self.functions = {}

    def tool(self):
        def decorator(fn):
            self.functions[fn.__name__] = fn
            return fn
        return decorator


class TrustPolicyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db_path = self.tmp.name
        self.original_db_path = db.DB_PATH
        db.DB_PATH = self.db_path
        self.mcp = FakeMCP()
        tools.register(self.mcp)
        self.sender = "peer-full"

    def tearDown(self):
        db.DB_PATH = self.original_db_path
        os.unlink(self.db_path)

    def call(self, name, **kwargs):
        return asyncio.run(self.mcp.functions[name](**kwargs))

    def test_schema_and_migration_is_idempotent(self):
        # A legacy database containing only messages is upgraded when opened.
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE messages (msg_id TEXT PRIMARY KEY, sender_handle TEXT, recipient_handle TEXT, text TEXT, metadata TEXT, timestamp TEXT, status TEXT)")
        with db.connection() as conn:
            columns = {row[1]: row for row in conn.execute("PRAGMA table_info(trusted_peers)")}
            self.assertEqual(set(columns), {"peer_id", "trust_level", "scopes", "auto_approve", "notify_owner", "updated_at"})
            self.assertEqual(columns["peer_id"][5], 1)  # primary key
            self.assertEqual(columns["trust_level"][3], 1)  # NOT NULL
            self.assertEqual(columns["auto_approve"][4], "0")
            self.assertEqual(columns["notify_owner"][4], "1")
        # Reopening must preserve the schema and not raise.
        with db.connection() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM trusted_peers").fetchone()[0], 0)

    def test_full_trust_persists_and_forces_auto_approve(self):
        with patch.object(tools, "caller", new=AsyncMock(return_value="owner")):
            result = self.call("set_trusted_peer", peer_id=self.sender, trust_level="full", auto_approve=False, notify_owner=True)
        self.assertIn(self.sender, result)
        with db.connection() as conn:
            row = dict(conn.execute("SELECT * FROM trusted_peers WHERE peer_id=?", (self.sender,)).fetchone())
        self.assertEqual(row["trust_level"], "full")
        self.assertEqual(row["auto_approve"], 1)
        self.assertEqual(row["notify_owner"], 1)
        self.assertEqual(json.loads(row["scopes"]), [])

    def test_full_trust_send_resolves_and_notifies_owner(self):
        with db.connection() as conn:
            conn.execute("INSERT INTO trusted_peers VALUES (?,?,?,?,?,?)", (self.sender, "full", "[]", 1, 1, "now"))
        notifications = []

        async def fake_push(recipient, message, sender, **kwargs):
            notifications.append((recipient, message, sender, kwargs))

        with patch.object(tools, "caller", new=AsyncMock(return_value=self.sender)), patch.object(tools, "push_to_poke", new=fake_push):
            msg_id = self.call("send_poke_message", recipient="owner", message="hello owner")
        self.assertTrue(msg_id)
        self.assertEqual(len(notifications), 1)
        recipient, payload, sender, kwargs = notifications[0]
        self.assertEqual(recipient, "owner")
        self.assertEqual(payload, "annoin peer-full: hello owner")
        self.assertEqual(sender, self.sender)
        self.assertEqual(kwargs["event"], "trust_notification")
        self.assertEqual(kwargs["status"], "auto_approved")
        with db.connection() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM messages WHERE msg_id=?", (msg_id,)).fetchone()[0], 1)

    def test_untrusted_peer_cannot_bypass_full_trust(self):
        notifications = []

        async def fake_push(recipient, message, sender, **kwargs):
            notifications.append((recipient, message, sender, kwargs))

        with patch.object(tools, "caller", new=AsyncMock(return_value="peer-none")), patch.object(tools, "push_to_poke", new=fake_push):
            msg_id = self.call("send_poke_message", recipient="owner", message="needs review")
            # Let the non-trusted branch's scheduled task run.
            asyncio.run(asyncio.sleep(0))
        self.assertTrue(msg_id)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0][1], "needs review")
        self.assertNotEqual(notifications[0][1], "annoin peer-none: needs review")
        self.assertEqual(notifications[0][3].get("event", "message"), "message")
        with db.connection() as conn:
            self.assertIsNone(conn.execute("SELECT 1 FROM trusted_peers WHERE peer_id='peer-none'").fetchone())


if __name__ == "__main__":
    unittest.main(verbosity=2)
