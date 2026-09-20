import sqlite3
from config import DB_PATH

def connection():
    c=sqlite3.connect(DB_PATH, timeout=5)
    c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA busy_timeout=5000'); c.execute('PRAGMA foreign_keys=ON')
    c.execute('CREATE TABLE IF NOT EXISTS messages (msg_id TEXT PRIMARY KEY,sender_handle TEXT,recipient_handle TEXT,text TEXT,metadata TEXT,timestamp TEXT,status TEXT)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_messages_recipient_status_time ON messages(recipient_handle,status,timestamp)')
    c.execute('''CREATE TABLE IF NOT EXISTS trusted_peers (
        peer_id TEXT PRIMARY KEY,
        trust_level TEXT NOT NULL CHECK (trust_level IN ('full','scoped','none')) DEFAULT 'none',
        scopes TEXT NOT NULL DEFAULT '[]',
        auto_approve INTEGER NOT NULL DEFAULT 0 CHECK (auto_approve IN (0,1)),
        notify_owner INTEGER NOT NULL DEFAULT 1 CHECK (notify_owner IN (0,1)),
        updated_at TEXT NOT NULL
    )''')
    return c
