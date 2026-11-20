"""SQLite fixture database for the settlement test dispatcher.

The shared database is a single SQLite file.  Audit cases are read-only and may
run concurrently; refund cases write to the ledger and therefore need exclusive
access.  To make the "restore to the starting state" step deterministic, the
refund dispatcher snapshots the database file before the child process starts
and swaps the snapshot back afterwards.

The seed is deliberately small and deterministic so tests can assert on exact
values.
"""

from __future__ import annotations

import shutil
import sqlite3
import threading
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id      TEXT PRIMARY KEY,
    merchant_id   TEXT NOT NULL,
    amount_cents  INTEGER NOT NULL CHECK (amount_cents >= 0),
    status        TEXT NOT NULL CHECK (status IN ('paid', 'refunded', 'partial_refund')),
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    entry_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id      TEXT NOT NULL REFERENCES orders(order_id),
    kind          TEXT NOT NULL CHECK (kind IN ('payment', 'refund')),
    amount_cents  INTEGER NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refund_requests (
    request_id    TEXT PRIMARY KEY,
    order_id      TEXT NOT NULL REFERENCES orders(order_id),
    amount_cents  INTEGER NOT NULL CHECK (amount_cents > 0),
    state         TEXT NOT NULL CHECK (state IN ('pending', 'applied')),
    created_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ledger_order ON ledger_entries(order_id);

PRAGMA user_version = 1;
"""

SEED_ORDERS = [
    ("ORD-1001", "MER-A", 12_500, "paid", "2026-01-02T09:00:00Z"),
    ("ORD-1002", "MER-A", 4_200, "paid", "2026-01-02T09:05:00Z"),
    ("ORD-1003", "MER-B", 30_000, "refunded", "2026-01-03T11:30:00Z"),
    ("ORD-1004", "MER-B", 8_900, "partial_refund", "2026-01-04T14:15:00Z"),
    ("ORD-1005", "MER-C", 1_999, "paid", "2026-01-05T08:45:00Z"),
]

SEED_LEDGER = [
    ("ORD-1001", "payment", 12_500, "2026-01-02T09:00:00Z"),
    ("ORD-1002", "payment", 4_200, "2026-01-02T09:05:00Z"),
    ("ORD-1003", "payment", 30_000, "2026-01-03T11:30:00Z"),
    ("ORD-1003", "refund", -30_000, "2026-01-04T10:00:00Z"),
    ("ORD-1004", "payment", 8_900, "2026-01-04T14:15:00Z"),
    ("ORD-1004", "refund", -1_000, "2026-01-05T16:00:00Z"),
    ("ORD-1005", "payment", 1_999, "2026-01-05T08:45:00Z"),
]

SEED_REFUND_REQUESTS = [
    ("RR-2001", "ORD-1003", 30_000, "applied", "2026-01-04T09:55:00Z"),
    ("RR-2002", "ORD-1004", 1_000, "applied", "2026-01-05T15:50:00Z"),
]


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=10.0)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = DELETE")
    return conn


def _connect_readonly(path: Path) -> sqlite3.Connection:
    """Open a connection that cannot write and adds no locks of its own."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10.0)
    conn.execute("PRAGMA query_only = ON")
    return conn


def build_seed_database(path: Path) -> None:
    """Create a fresh seed database at ``path`` (replacing any existing file)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm", "-journal"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()
    conn = _connect(path)
    try:
        with conn:
            conn.executescript(SCHEMA)
            conn.executemany(
                "INSERT INTO orders (order_id, merchant_id, amount_cents, status, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                SEED_ORDERS,
            )
            conn.executemany(
                "INSERT INTO ledger_entries (order_id, kind, amount_cents, created_at)"
                " VALUES (?, ?, ?, ?)",
                SEED_LEDGER,
            )
            conn.executemany(
                "INSERT INTO refund_requests"
                " (request_id, order_id, amount_cents, state, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                SEED_REFUND_REQUESTS,
            )
    finally:
        conn.close()


def database_fingerprint(path: Path) -> dict:
    """Return a compact, comparable view of the data in the database."""
    conn = _connect_readonly(path)
    try:
        orders = conn.execute(
            "SELECT order_id, merchant_id, amount_cents, status, created_at"
            " FROM orders ORDER BY order_id"
        ).fetchall()
        ledger = conn.execute(
            "SELECT entry_id, order_id, kind, amount_cents, created_at"
            " FROM ledger_entries ORDER BY entry_id"
        ).fetchall()
        refunds = conn.execute(
            "SELECT request_id, order_id, amount_cents, state, created_at"
            " FROM refund_requests ORDER BY request_id"
        ).fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        conn.close()
    return {
        "orders": [list(row) for row in orders],
        "ledger_entries": [list(row) for row in ledger],
        "refund_requests": [list(row) for row in refunds],
        "integrity": integrity,
    }


class SeedManager:
    """Owns the canonical seed snapshot and copies it to the live DB on demand."""

    def __init__(self, settings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._seed_fingerprint: dict | None = None

    def bootstrap(self) -> None:
        """Ensure the canonical snapshot exists and the live DB starts clean."""
        self._settings.data_dir.mkdir(parents=True, exist_ok=True)
        if not self._settings.seed_snapshot_path.exists():
            build_seed_database(self._settings.seed_snapshot_path)
        # The dispatcher keeps its queue in memory, so a service restart always
        # begins from the canonical seed rather than from a possibly dirty file.
        self.restore_from(self._settings.seed_snapshot_path)

    def restore(self) -> None:
        """Restore the live database from the canonical seed snapshot."""
        self.restore_from(self._settings.seed_snapshot_path)

    def restore_from(self, source: Path) -> None:
        """Atomically put the live database back to the state in ``source``."""
        if not source.exists():
            raise FileNotFoundError(f"恢复来源不存在: {source}")
        with self._lock:
            target = self._settings.db_path
            target.parent.mkdir(parents=True, exist_ok=True)
            staging = target.with_name(target.name + ".restore-stage")
            if staging.exists():
                staging.unlink()
            # Copy into staging, verify, then rename into place so readers never
            # observe a half-written database file.
            shutil.copyfile(source, staging)
            fingerprint = database_fingerprint(staging)
            if fingerprint["integrity"] != "ok":
                staging.unlink(missing_ok=True)
                raise RuntimeError("待恢复快照未通过 SQLite integrity_check")
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = Path(str(target) + suffix)
                if sidecar.exists():
                    sidecar.unlink()
            staging.replace(target)

    def seed_fingerprint(self) -> dict:
        """Fingerprint of the canonical seed, read once (used for drift checks)."""
        with self._lock:
            if self._seed_fingerprint is None:
                self._seed_fingerprint = database_fingerprint(self._settings.seed_snapshot_path)
            return self._seed_fingerprint

    def read_fingerprint(self) -> dict:
        return database_fingerprint(self._settings.db_path)

    def matches_seed(self) -> bool:
        """True when the live database equals the canonical starting state."""
        return database_fingerprint(self._settings.db_path) == self.seed_fingerprint()
