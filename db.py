import contextlib
import sqlite3
import threading
from config import DB_PATH

_init_lock = threading.Lock()
_initialized = False

SCHEMA = (
    'CREATE TABLE IF NOT EXISTS messages ('
    'msg_id TEXT PRIMARY KEY, sender_handle TEXT NOT NULL, recipient_handle TEXT NOT NULL, '
    "text TEXT NOT NULL DEFAULT '', metadata TEXT NOT NULL DEFAULT '{}', timestamp TEXT NOT NULL, "
    "status TEXT NOT NULL DEFAULT 'unread', acknowledged_at TEXT)",
    'CREATE INDEX IF NOT EXISTS idx_messages_recipient_status_time ON messages(recipient_handle,status,timestamp)',
)


def _migrate(c):
    cols = {r['name'] for r in c.execute('PRAGMA table_info(messages)')}
    if cols and 'acknowledged_at' not in cols:
        c.execute('ALTER TABLE messages ADD COLUMN acknowledged_at TEXT')


def _connect():
    c = sqlite3.connect(DB_PATH, timeout=5, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA busy_timeout=5000')
    c.execute('PRAGMA foreign_keys=ON')
    return c


def _ensure_schema():
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        with contextlib.closing(_connect()) as c:
            c.execute('PRAGMA journal_mode=WAL')
            _migrate(c)
            for stmt in SCHEMA:
                c.execute(stmt)
        _initialized = True


@contextlib.contextmanager
def connection():
    """Yield a connection inside a transaction; commits on success, rolls back on error, always closes."""
    _ensure_schema()
    c = _connect()
    try:
        c.execute('BEGIN IMMEDIATE')
        yield c
        c.execute('COMMIT')
    except BaseException:
        with contextlib.suppress(sqlite3.Error):
            c.execute('ROLLBACK')
        raise
    finally:
        c.close()
