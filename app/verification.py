"""Write verification for refund cases.

Refund cases are allowed to write to the settlement database, but only for the
order(s) the case is *targeting*.  A typo such as ``WHERE order_id = 'ORD-1002'``
inside a case that should touch ``ORD-1001`` could easily go unnoticed: the
case still passes its own assertions, and the dispatcher restores the database
afterwards anyway.  This module makes those stray writes visible.

How it works
------------

Immediately before the refund child process is started, ``arm`` installs
``AFTER INSERT/UPDATE/DELETE`` triggers on the three settlement tables.  The
triggers copy the affected row images (old and new) into a journal table in the
same SQLite database and stamp each entry with the dispatching task's id.

Using triggers in the same database gives exactly the semantics we need:

* A write that is rolled back also rolls back its journal entry, so rolled-back
  writes are *not* counted.
* A committed write is durable before the child exits, so evidence survives a
  process crash that happens after ``COMMIT``.
* An ``UPDATE`` trigger fires even when the new values equal the old ones, and
  an update is journaled with both the old and the new ``order_id``.  A case
  that writes a non-target order and then changes it back still leaves two
  journal entries behind.
* The journal carries the task id, so evidence is attributable to the case that
  produced it even if the process died before printing a report.

Immediately after the child exits -- and always before the database is restored
from the seed snapshot -- ``capture`` reads the journal and ``evaluate`` decides
whether the case stayed inside its target orders.  A capture that cannot be
completed yields the ``unverified`` status: the case is not allowed to pass, but
the dispatcher still restores the database.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal

VerificationStatus = Literal["passed", "failed", "unverified"]

SESSION_TABLE = "__verify_session"
JOURNAL_TABLE = "__verify_writes"

# The tables the refund presets are allowed to write to, with the column that
# identifies a row and the column that links a row to an order.
TABLE_SPECS: dict[str, dict[str, Any]] = {
    "orders": {
        "key": "order_id",
        "columns": ("order_id", "merchant_id", "amount_cents", "status", "created_at"),
    },
    "ledger_entries": {
        "key": "entry_id",
        "columns": ("entry_id", "order_id", "kind", "amount_cents", "created_at"),
    },
    "refund_requests": {
        "key": "request_id",
        "columns": ("request_id", "order_id", "amount_cents", "state", "created_at"),
    },
}


class VerificationError(RuntimeError):
    """Raised when evidence cannot be armed, read or trusted."""


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=15.0, isolation_level=None)
    conn.execute("PRAGMA busy_timeout = 15000")
    return conn


def _json_object(prefix: str, columns: Iterable[str]) -> str:
    pairs = ", ".join(f"'{column}', {prefix}.{column}" for column in columns)
    return f"json_object({pairs})"


def _session_value() -> str:
    return f"(SELECT task_id FROM {SESSION_TABLE} WHERE id = 1)"


def _trigger_sql(table: str, spec: dict[str, Any]) -> list[str]:
    key = spec["key"]
    columns = spec["columns"]
    statements: list[str] = []
    for suffix, operation in (("ai", "insert"), ("au", "update"), ("ad", "delete")):
        statements.append(f"DROP TRIGGER IF EXISTS __verify_{table}_{suffix}")

    # INSERT: only the new row exists; the affected order is NEW.order_id.
    statements.append(
        f"""
        CREATE TRIGGER __verify_{table}_ai AFTER INSERT ON {table} BEGIN
          INSERT INTO {JOURNAL_TABLE}
            (task_id, table_name, operation, row_key, old_order_id, new_order_id,
             old_row, new_row, created_at)
          VALUES (
            {_session_value()}, '{table}', 'insert', NEW.{key}, NULL, NEW.order_id,
            NULL, {_json_object("NEW", columns)},
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          );
        END
        """
    )
    # UPDATE: fires even when the values are unchanged, and records *both* the
    # old and the new order id so a re-keyed row cannot hide from the check.
    statements.append(
        f"""
        CREATE TRIGGER __verify_{table}_au AFTER UPDATE ON {table} BEGIN
          INSERT INTO {JOURNAL_TABLE}
            (task_id, table_name, operation, row_key, old_order_id, new_order_id,
             old_row, new_row, created_at)
          VALUES (
            {_session_value()}, '{table}', 'update', NEW.{key},
            OLD.order_id, NEW.order_id,
            {_json_object("OLD", columns)}, {_json_object("NEW", columns)},
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          );
        END
        """
    )
    # DELETE: only the old row exists; the affected order is OLD.order_id.
    statements.append(
        f"""
        CREATE TRIGGER __verify_{table}_ad AFTER DELETE ON {table} BEGIN
          INSERT INTO {JOURNAL_TABLE}
            (task_id, table_name, operation, row_key, old_order_id, new_order_id,
             old_row, new_row, created_at)
          VALUES (
            {_session_value()}, '{table}', 'delete', OLD.{key},
            OLD.order_id, NULL,
            {_json_object("OLD", columns)}, NULL,
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          );
        END
        """
    )
    return statements


def arm(db_path: Path, task_id: str, preset_id: str, target_orders: Iterable[str]) -> None:
    """Install the write journal and bind it to ``task_id``.

    Must be called after the previous case has been restored and before the
    child process starts.
    """
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SESSION_TABLE} (
                    id            INTEGER PRIMARY KEY CHECK (id = 1),
                    task_id       TEXT NOT NULL,
                    preset_id     TEXT NOT NULL,
                    target_orders TEXT NOT NULL,
                    armed_at      TEXT NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {JOURNAL_TABLE} (
                    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id       TEXT,
                    table_name    TEXT NOT NULL,
                    operation     TEXT NOT NULL,
                    row_key       TEXT,
                    old_order_id  TEXT,
                    new_order_id  TEXT,
                    old_row       TEXT,
                    new_row       TEXT,
                    created_at    TEXT NOT NULL
                )
                """
            )
            # A stale journal from an earlier run must never be attributed to
            # this case.
            conn.execute(f"DELETE FROM {JOURNAL_TABLE}")
            conn.execute(f"DELETE FROM {SESSION_TABLE}")
            conn.execute(
                f"INSERT INTO {SESSION_TABLE} (id, task_id, preset_id, target_orders, armed_at)"
                " VALUES (1, ?, ?, ?, ?)",
                (
                    task_id,
                    preset_id,
                    json.dumps(sorted(set(target_orders)), ensure_ascii=False),
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                ),
            )
            for table, spec in TABLE_SPECS.items():
                for statement in _trigger_sql(table, spec):
                    conn.execute(statement)
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def _parse_row(raw: str | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {"_raw": raw}
    return value if isinstance(value, dict) else {"_value": value}


def capture(db_path: Path) -> dict[str, Any]:
    """Read the journal after the child exited and before the restore.

    Opening the database read-write is intentional: if the child was killed
    mid-transaction, SQLite rolls the hot journal back here, which is exactly
    the "rolled-back writes do not count" rule.
    """
    conn = _connect(db_path)
    try:
        existing = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        if SESSION_TABLE not in existing or JOURNAL_TABLE not in existing:
            # Nothing was ever armed for this database: no evidence, not an error.
            return {"session": None, "writes": [], "captured_at": time.time()}
        try:
            session_row = conn.execute(
                f"SELECT task_id, preset_id, target_orders, armed_at FROM {SESSION_TABLE}"
                " WHERE id = 1"
            ).fetchone()
            rows = conn.execute(
                f"SELECT seq, task_id, table_name, operation, row_key, old_order_id,"
                f" new_order_id, old_row, new_row, created_at"
                f" FROM {JOURNAL_TABLE} ORDER BY seq"
            ).fetchall()
        except sqlite3.Error as exc:
            raise VerificationError(f"无法读取写入取证日志: {exc}") from exc
    finally:
        conn.close()

    session = (
        {
            "task_id": session_row[0],
            "preset_id": session_row[1],
            "target_orders": json.loads(session_row[2]),
            "armed_at": session_row[3],
        }
        if session_row is not None
        else None
    )
    writes = [
        {
            "seq": row[0],
            "task_id": row[1],
            "table": row[2],
            "operation": row[3],
            "row_key": row[4],
            "old_order_id": row[5],
            "new_order_id": row[6],
            "old_row": _parse_row(row[7]),
            "new_row": _parse_row(row[8]),
            "written_at": row[9],
        }
        for row in rows
    ]
    return {"session": session, "writes": writes, "captured_at": time.time()}


def _involved_order_ids(write: dict[str, Any]) -> list[str]:
    """Order ids a write touches, checking both sides of an update."""
    operation = write["operation"]
    if operation == "insert":
        candidates = [write["new_order_id"]]
    elif operation == "delete":
        candidates = [write["old_order_id"]]
    else:
        # An update that re-keys a row touches the old *and* the new order.
        candidates = [write["old_order_id"], write["new_order_id"]]
    return [order_id for order_id in candidates if order_id is not None]


def _base_report(task_id: str, preset_id: str, target_orders: Iterable[str]) -> dict[str, Any]:
    return {
        "status": "unverified",
        "verified": False,
        "checked_before_restore": True,
        "task_id": task_id,
        "preset_id": preset_id,
        "target_orders": sorted(set(target_orders)),
        "write_count": 0,
        "writes": [],
        "violations": [],
        "violating_tables": [],
        "violating_order_ids": [],
        "error": None,
    }


def unverified(
    task_id: str, preset_id: str, target_orders: Iterable[str], error: str
) -> dict[str, Any]:
    """Report for evidence that could not be armed, read or attributed."""
    report = _base_report(task_id, preset_id, target_orders)
    report["error"] = error
    return report


def evaluate(
    captured: dict[str, Any],
    task_id: str,
    preset_id: str,
    target_orders: Iterable[str],
) -> dict[str, Any]:
    """Decide whether the captured writes stayed inside the target orders."""
    targets = set(target_orders)
    report = _base_report(task_id, preset_id, targets)

    session = captured.get("session")
    if session is None:
        report["error"] = "取证会话缺失，无法将写入归到本次用例"
        return report
    if session.get("task_id") != task_id:
        report["error"] = (
            f"取证会话属于其他用例（{session.get('task_id')}），无法归到本次用例"
        )
        return report

    writes = captured.get("writes", [])
    foreign = [write for write in writes if write.get("task_id") != task_id]
    if foreign:
        report["error"] = f"取证日志中有 {len(foreign)} 条写入不属于本次用例，无法归因"
        report["writes"] = writes
        report["write_count"] = len(writes)
        return report

    violations: list[dict[str, Any]] = []
    for write in writes:
        involved = _involved_order_ids(write)
        stray = [order_id for order_id in involved if order_id not in targets]
        if stray:
            violations.append(
                {
                    "seq": write["seq"],
                    "table": write["table"],
                    "operation": write["operation"],
                    "row_key": write["row_key"],
                    "order_ids": stray,
                    "old_order_id": write["old_order_id"],
                    "new_order_id": write["new_order_id"],
                    "written_at": write["written_at"],
                }
            )

    report["writes"] = writes
    report["write_count"] = len(writes)
    report["violations"] = violations
    report["violating_tables"] = sorted({item["table"] for item in violations})
    report["violating_order_ids"] = sorted(
        {order_id for item in violations for order_id in item["order_ids"]}
    )
    report["status"] = "failed" if violations else "passed"
    report["verified"] = True
    return report


def summary_message(report: dict[str, Any]) -> str:
    """Short human-readable Chinese summary for the task message/UI."""
    status = report.get("status")
    if status == "passed":
        target = "、".join(report.get("target_orders", [])) or "-"
        return f"核验通过：仅写入目标订单 {target}（{report.get('write_count', 0)} 条写入）"
    if status == "failed":
        detail = "；".join(
            f"{table}（订单 {'、'.join(sorted({o for v in report.get('violations', []) if v['table'] == table for o in v['order_ids']}))}）"
            for table in report.get("violating_tables", [])
        )
        return f"核验失败：发现非目标订单写入 —— {detail}"
    return f"核验未完成（未核验）：{report.get('error') or '未知原因'}"


def combine_result(result: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    """Fold verification into the child result.

    The case may only pass when the worker passed *and* verification passed.
    An unverified case can never pass, even if the worker reported success.
    """
    worker_passed = bool(result.get("passed"))
    verification_passed = report.get("status") == "passed"
    passed = worker_passed and verification_passed

    message = str(result.get("message") or "")
    if report.get("status") != "passed":
        suffix = summary_message(report)
        if worker_passed and message == "通过":
            # The business assertions passed, but the case still fails because
            # the writes could not be cleared; say so without implying success.
            message = f"退款用例执行成功；{suffix}"
        else:
            message = f"{message}；{suffix}" if message else suffix

    combined = dict(result)
    combined["passed"] = passed
    combined["worker_passed"] = worker_passed
    combined["verification"] = report
    combined["message"] = message
    return combined
