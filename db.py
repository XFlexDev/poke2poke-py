import sqlite3
from config import DB_PATH

def connection():
    c=sqlite3.connect(DB_PATH, timeout=5)
    c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA busy_timeout=5000'); c.execute('PRAGMA foreign_keys=ON')
    c.execute('CREATE TABLE IF NOT EXISTS messages (msg_id TEXT PRIMARY KEY,sender_handle TEXT,recipient_handle TEXT,text TEXT,metadata TEXT,timestamp TEXT,status TEXT)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_messages_recipient_status_time ON messages(recipient_handle,status,timestamp)')
    return c
