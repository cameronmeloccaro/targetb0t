import asyncio
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "targetb0t.db"

_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS proxy_lists (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS proxies (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    list_id      INTEGER NOT NULL,
    url          TEXT NOT NULL,
    enabled      INTEGER NOT NULL DEFAULT 1,
    fail_count   INTEGER NOT NULL DEFAULT 0,
    last_used_at TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (list_id) REFERENCES proxy_lists(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS accounts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    nickname          TEXT    NOT NULL UNIQUE,
    email             TEXT,
    access_token      TEXT    NOT NULL DEFAULT '',
    refresh_token     TEXT,
    expires_at        TEXT,
    checkout_cookies  TEXT    NOT NULL DEFAULT '',
    password          TEXT    NOT NULL DEFAULT '',
    ccv               TEXT    NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    nickname         TEXT    NOT NULL,
    tcin             TEXT    NOT NULL,
    store_id         TEXT,
    interval_seconds INTEGER NOT NULL DEFAULT 10,
    status           TEXT    NOT NULL DEFAULT 'active'
                             CHECK(status IN ('active','paused','in_cart','checkout','error')),
    proxy_list_id    INTEGER,
    account_id       INTEGER,
    visitor_id       TEXT,
    live_status      TEXT    NOT NULL DEFAULT '',
    last_checked_at  TEXT,
    last_in_stock_at TEXT,
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (proxy_list_id) REFERENCES proxy_lists(id) ON DELETE SET NULL,
    FOREIGN KEY (account_id)    REFERENCES accounts(id)    ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL,
    event_type  TEXT    NOT NULL,
    detail      TEXT,
    occurred_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_events_task_id ON events(task_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_proxies_list_id ON proxies(list_id, last_used_at);
"""


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


_MIGRATIONS = [
    "ALTER TABLE accounts ADD COLUMN email TEXT",
    "ALTER TABLE accounts ADD COLUMN expires_at TEXT",
    "ALTER TABLE tasks ADD COLUMN live_status TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE accounts ADD COLUMN checkout_cookies TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE accounts ADD COLUMN password TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE accounts ADD COLUMN ccv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE tasks ADD COLUMN quantity INTEGER NOT NULL DEFAULT 1",
]


def _migrate_tasks_checkout(conn: sqlite3.Connection) -> None:
    """Recreate the tasks table with 'checkout' in the status CHECK constraint."""
    # Check the actual CREATE TABLE statement for 'checkout'
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='tasks'"
    ).fetchone()
    if not row:
        return  # table doesn't exist yet — will be created fresh by _SCHEMA
    if "checkout" in row[0]:
        return  # constraint already includes 'checkout'

    conn.executescript("""
        CREATE TABLE tasks_new (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            nickname         TEXT    NOT NULL,
            tcin             TEXT    NOT NULL,
            store_id         TEXT,
            interval_seconds INTEGER NOT NULL DEFAULT 10,
            status           TEXT    NOT NULL DEFAULT 'active'
                                     CHECK(status IN ('active','paused','in_cart','checkout','error')),
            proxy_list_id    INTEGER,
            account_id       INTEGER,
            visitor_id       TEXT,
            last_checked_at  TEXT,
            last_in_stock_at TEXT,
            created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            FOREIGN KEY (proxy_list_id) REFERENCES proxy_lists(id) ON DELETE SET NULL,
            FOREIGN KEY (account_id)    REFERENCES accounts(id)    ON DELETE SET NULL
        );
        INSERT INTO tasks_new SELECT * FROM tasks;
        DROP TABLE tasks;
        ALTER TABLE tasks_new RENAME TO tasks;
    """)


def init_db() -> None:
    conn = _get_conn()
    try:
        conn.executescript(_SCHEMA)
        for stmt in _MIGRATIONS:
            try:
                conn.execute(stmt)
                conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists
        _migrate_tasks_checkout(conn)
    finally:
        conn.close()


def _run_fetch_all(query: str, params: tuple) -> list[dict]:
    conn = _get_conn()
    try:
        cur = conn.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _run_fetch_one(query: str, params: tuple) -> dict | None:
    conn = _get_conn()
    try:
        cur = conn.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _run_execute(query: str, params: tuple) -> int:
    conn = _get_conn()
    try:
        cur = conn.execute(query, params)
        conn.commit()
        return cur.lastrowid or 0
    finally:
        conn.close()


async def fetch_all(query: str, params: tuple = ()) -> list[dict]:
    return await asyncio.to_thread(_run_fetch_all, query, params)


async def fetch_one(query: str, params: tuple = ()) -> dict | None:
    return await asyncio.to_thread(_run_fetch_one, query, params)


async def execute(query: str, params: tuple = ()) -> int:
    return await asyncio.to_thread(_run_execute, query, params)
