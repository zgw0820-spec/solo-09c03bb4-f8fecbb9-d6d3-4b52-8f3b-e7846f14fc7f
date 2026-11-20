"""Standalone worker executed in its own OS process for every test case.

Invoked as::

    python -m app.worker --case AUD-001 --db /path/to/settlement.db

The process is intentionally independent from the web server: it opens the
SQLite file directly, runs the preset, prints a JSON report on stdout, and
exits with a status code that reflects the case outcome (0 = passed).

Refund presets run inside a single ``BEGIN IMMEDIATE`` transaction so a
concurrent audit cannot observe a half-applied refund.  Exiting with a
non-zero code does not roll back an already-committed refund; the dispatcher
is responsible for restoring the database afterwards.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from collections.abc import Callable
from pathlib import Path

if __package__ in (None, ""):  # ``python app/worker.py`` support
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.cases import PRESETS_BY_ID, CaseDefinition  # noqa: E402


class CaseFailure(AssertionError):
    """Raised when a preset detects a business-rule violation."""


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=15.0, isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 15000")
    return conn


def _rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[tuple]:
    return conn.execute(sql, params).fetchall()


def _hold(hold_ms: int) -> None:
    """Keep the worker alive so concurrency can be observed in tests."""
    if hold_ms > 0:
        time.sleep(hold_ms / 1000)


# ---------------------------------------------------------------------------
# Audit presets (read-only)
# ---------------------------------------------------------------------------


def audit_order_totals(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    _hold(hold_ms)
    paid = dict(
        _rows(
            conn,
            "SELECT order_id, SUM(amount_cents) FROM ledger_entries"
            " WHERE kind = 'payment' GROUP BY order_id",
        )
    )
    mismatches = []
    for order_id, amount in _rows(conn, "SELECT order_id, amount_cents FROM orders"):
        if paid.get(order_id, 0) != amount:
            mismatches.append(
                {"order_id": order_id, "order_cents": amount, "payment_cents": paid.get(order_id, 0)}
            )
    if mismatches:
        raise CaseFailure(f"订单金额与支付流水不一致: {json.dumps(mismatches)}")
    return {"orders_checked": len(paid), "mismatches": 0}


def audit_refund_within_payment(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    _hold(hold_ms)
    rows = _rows(
        conn,
        "SELECT order_id,"
        " SUM(CASE WHEN kind = 'payment' THEN amount_cents ELSE 0 END) AS paid,"
        " SUM(CASE WHEN kind = 'refund' THEN -amount_cents ELSE 0 END) AS refunded"
        " FROM ledger_entries GROUP BY order_id",
    )
    over = [
        {"order_id": oid, "paid_cents": paid, "refunded_cents": refunded}
        for oid, paid, refunded in rows
        if refunded > paid
    ]
    if over:
        raise CaseFailure(f"退款流水超过支付流水: {json.dumps(over)}")
    return {"orders_checked": len(rows), "violations": 0}


def audit_status_consistency(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    _hold(hold_ms)
    rows = _rows(
        conn,
        "SELECT o.order_id, o.status, o.amount_cents,"
        " COALESCE(SUM(CASE WHEN l.kind = 'refund' THEN -l.amount_cents ELSE 0 END), 0)"
        " FROM orders o LEFT JOIN ledger_entries l ON l.order_id = o.order_id"
        " GROUP BY o.order_id, o.status, o.amount_cents",
    )
    bad = []
    for order_id, status, amount, refunded in rows:
        net = amount - refunded
        if status == "paid" and refunded != 0:
            bad.append({"order_id": order_id, "status": status, "refunded_cents": refunded})
        elif status == "refunded" and net != 0:
            bad.append({"order_id": order_id, "status": status, "net_cents": net})
        elif status == "partial_refund" and not (0 < refunded < amount):
            bad.append({"order_id": order_id, "status": status, "refunded_cents": refunded})
    if bad:
        raise CaseFailure(f"订单状态与净额不一致: {json.dumps(bad)}")
    return {"orders_checked": len(rows), "violations": 0}


def audit_refund_requests_match_ledger(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    _hold(hold_ms)
    applied = dict(
        _rows(
            conn,
            "SELECT order_id, SUM(amount_cents) FROM refund_requests"
            " WHERE state = 'applied' GROUP BY order_id",
        )
    )
    refunded = dict(
        _rows(
            conn,
            "SELECT order_id, SUM(-amount_cents) FROM ledger_entries"
            " WHERE kind = 'refund' GROUP BY order_id",
        )
    )
    mismatches = []
    for order_id in sorted(set(applied) | set(refunded)):
        if applied.get(order_id, 0) != refunded.get(order_id, 0):
            mismatches.append(
                {
                    "order_id": order_id,
                    "requests_cents": applied.get(order_id, 0),
                    "ledger_cents": refunded.get(order_id, 0),
                }
            )
    if mismatches:
        raise CaseFailure(f"退款申请与退款流水不一致: {json.dumps(mismatches)}")
    return {"orders_checked": len(applied), "mismatches": 0}


# ---------------------------------------------------------------------------
# Refund presets (write, exclusive)
# ---------------------------------------------------------------------------


def _refund(
    conn: sqlite3.Connection,
    order_id: str,
    amount_cents: int,
    request_id: str,
    hold_ms: int = 0,
) -> dict:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT amount_cents, status FROM orders WHERE order_id = ?", (order_id,)
        ).fetchone()
        if row is None:
            raise CaseFailure(f"订单不存在: {order_id}")
        order_amount, _ = row
        refunded = conn.execute(
            "SELECT COALESCE(SUM(-amount_cents), 0) FROM ledger_entries"
            " WHERE order_id = ? AND kind = 'refund'",
            (order_id,),
        ).fetchone()[0]
        remaining = order_amount - refunded
        if amount_cents <= 0:
            raise CaseFailure("退款金额必须大于 0")
        if amount_cents > remaining:
            raise CaseFailure(
                f"退款金额 {amount_cents} 超过可退余额 {remaining}（订单 {order_id}）"
            )
        conn.execute(
            "INSERT INTO refund_requests"
            " (request_id, order_id, amount_cents, state, created_at)"
            " VALUES (?, ?, ?, 'applied', ?)",
            (request_id, order_id, amount_cents, now),
        )
        conn.execute(
            "INSERT INTO ledger_entries (order_id, kind, amount_cents, created_at)"
            " VALUES (?, 'refund', ?, ?)",
            (order_id, -amount_cents, now),
        )
        new_status = "refunded" if amount_cents == remaining else "partial_refund"
        conn.execute(
            "UPDATE orders SET status = ? WHERE order_id = ?", (new_status, order_id)
        )
        _hold(hold_ms)
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return {
        "order_id": order_id,
        "refunded_cents": amount_cents,
        "order_status": new_status,
        "remaining_cents": remaining - amount_cents,
    }


def refund_partial(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    return _refund(conn, "ORD-1001", 2_500, "RR-T-1001", hold_ms)


def refund_full(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    return _refund(conn, "ORD-1002", 4_200, "RR-T-1002", hold_ms)


def refund_reject_duplicate(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    # ORD-1003 is already fully refunded; this must raise.
    result = _refund(conn, "ORD-1003", 1_000, "RR-T-1003", hold_ms)
    raise CaseFailure(f"重复退款本应被拒绝却成功写入: {json.dumps(result)}")


def refund_reject_over_limit(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    result = _refund(conn, "ORD-1005", 2_500, "RR-T-1005", hold_ms)
    raise CaseFailure(f"超额退款本应被拒绝却成功写入: {json.dumps(result)}")


def refund_commit_then_crash(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    result = _refund(conn, "ORD-1005", 500, "RR-T-1005X", hold_ms)
    raise RuntimeError(f"用例在提交退款后故意异常退出: {json.dumps(result)}")


def refund_stray_write_restored(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    """Positive demo: refund the target, *also* touch a different order and undo it.

    The stray update is made inside the same transaction, reverted to the
    original value and committed.  The net effect on the database is nil, so
    only the write journal can tell that ORD-1004 was ever written.  The case
    is expected to fail verification even though the refund itself succeeded.
    """
    target = _refund(conn, "ORD-1002", 4_200, "RR-T-1002D", hold_ms)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT state FROM refund_requests WHERE request_id = 'RR-2002'"
        ).fetchone()
        if row is None:
            raise CaseFailure("演示用例缺少 ORD-1004 的退款申请 RR-2002")
        original_state = row[0]
        # Mistyped write: touches ORD-1004 instead of the target ORD-1002.
        conn.execute(
            "UPDATE refund_requests SET state = 'pending', created_at = ?"
            " WHERE order_id = 'ORD-1004'",
            (now,),
        )
        _hold(hold_ms)
        # Put the row back exactly as it was, then commit.  A value comparison
        # would call this a no-op; the journal still reports the write.
        conn.execute(
            "UPDATE refund_requests SET state = ?, created_at = '2026-01-05T15:50:00Z'"
            " WHERE request_id = 'RR-2002'",
            (original_state,),
        )
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return {**target, "stray_order_id": "ORD-1004", "stray_writes_reverted": True}


EXECUTORS: dict[str, Callable[..., dict]] = {
    "AUD-001": audit_order_totals,
    "AUD-002": audit_refund_within_payment,
    "AUD-003": audit_status_consistency,
    "AUD-004": audit_refund_requests_match_ledger,
    "REF-001": refund_partial,
    "REF-002": refund_full,
    "REF-003": refund_reject_duplicate,
    "REF-004": refund_reject_over_limit,
    "REF-005": refund_commit_then_crash,
    "REF-006": refund_stray_write_restored,
}

# Self-test hooks.  They are deliberately *not* submit-able presets: they exist
# so automated tests (and the manual Docker self-test described in the README)
# can provoke a hard crash after a stray write.  Invoke them with
# ``python -m app.worker --case SELFTEST-... --db ...``.
SELFTEST_EXECUTORS: dict[str, Callable[..., dict]] = {
    # Commit a write to a non-target order, then die via os._exit(3) so no
    # worker report is printed.  The journal row survives the crash.
    "SELFTEST-STRAY-COMMIT-CRASH": lambda conn, hold_ms=0: _stray_commit_hard_crash(conn),
    # Start a transaction that writes a non-target order and die *before*
    # COMMIT.  SQLite rolls it back on the next open, so it must not be counted.
    "SELFTEST-STRAY-ROLLBACK-CRASH": lambda conn, hold_ms=0: _stray_rollback_hard_crash(conn),
    # Commit a stray write and then a successful target refund, then die.
    "SELFTEST-STRAY-RESTORED-COMMIT-CRASH": lambda conn, hold_ms=0: _stray_restored_hard_crash(conn),
    # Write a non-target order, roll it back, then return normally.  The worker
    # passes and the rolled-back write must not be counted.
    "SELFTEST-STRAY-ROLLBACK-SUCCESS": lambda conn, hold_ms=0: _stray_rollback_then_pass(conn),
}


def _stray_commit_hard_crash(conn: sqlite3.Connection) -> dict:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE orders SET status = 'partial_refund' WHERE order_id = 'ORD-1004'"
    )
    conn.execute("COMMIT")
    os._exit(3)  # hard crash: no report, no cleanup


def _stray_rollback_hard_crash(conn: sqlite3.Connection) -> dict:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE orders SET status = 'partial_refund' WHERE order_id = 'ORD-1004'"
    )
    os._exit(3)  # die mid-transaction; SQLite must roll this back on next open


def _stray_restored_hard_crash(conn: sqlite3.Connection, hold_ms: int = 0) -> dict:
    refund_stray_write_restored(conn, hold_ms)
    os._exit(3)  # committed stray writes, then a hard crash


def _stray_rollback_then_pass(conn: sqlite3.Connection) -> dict:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE orders SET status = 'partial_refund' WHERE order_id = 'ORD-1004'"
    )
    conn.execute("ROLLBACK")
    # Give the case a real (target-order) write as well, so it is a normal pass.
    return refund_partial(conn)


def _executor(case_id: str) -> Callable[..., dict] | None:
    return EXECUTORS.get(case_id) or SELFTEST_EXECUTORS.get(case_id)


# Minimal definitions so ``run_case`` accepts the self-test ids as write cases.
_SELFTEST_DEFINITIONS: dict[str, CaseDefinition] = {
    case_id: CaseDefinition(
        preset_id=case_id,
        name="自检：误写后硬崩溃",
        kind="refund",
        description="仅用于自动化/人工自检，不通过网页预置列表暴露。",
        expectation="success",
        touches="写入非目标订单后 os._exit",
    )
    for case_id in SELFTEST_EXECUTORS
}


def _definition(case_id: str) -> CaseDefinition | None:
    return PRESETS_BY_ID.get(case_id) or _SELFTEST_DEFINITIONS.get(case_id)


def run_case(
    case_id: str, db_path: Path, hold_ms: int = 0
) -> tuple[bool, str, dict]:
    """Execute one preset and return ``(passed, message, details)``.

    ``hold_ms`` keeps the child alive (inside the refund transaction for write
    presets) so tests can observe concurrency and exclusivity deterministically.
    """
    definition = _definition(case_id)
    if definition is None:
        return False, f"未知预置用例: {case_id}", {}

    started = time.monotonic()
    conn: sqlite3.Connection | None = None
    try:
        conn = _connect(db_path)
        if definition.kind == "audit":
            # Enforce the read-only contract at the SQLite level: an audit that
            # accidentally writes fails instead of corrupting the shared DB.
            conn.execute("PRAGMA query_only = ON")
        conn.execute("SELECT 1 FROM orders LIMIT 1").fetchall()
        details = _executor(case_id)(conn, hold_ms=hold_ms)
        elapsed = int((time.monotonic() - started) * 1000)
        details["elapsed_ms"] = elapsed
        if definition.expectation == "failure":
            return False, "用例预期失败但实际通过", details
        return True, "通过", details
    except CaseFailure as exc:
        elapsed = int((time.monotonic() - started) * 1000)
        if definition.expectation == "failure":
            return True, f"按预期被拒绝: {exc}", {"elapsed_ms": elapsed}
        return False, f"断言失败: {exc}", {"elapsed_ms": elapsed}
    except sqlite3.Error as exc:
        elapsed = int((time.monotonic() - started) * 1000)
        return False, f"数据库错误: {exc}", {"elapsed_ms": elapsed}
    except Exception as exc:  # noqa: BLE001 - worker must always report
        elapsed = int((time.monotonic() - started) * 1000)
        return False, f"{type(exc).__name__}: {exc}", {"elapsed_ms": elapsed}
    finally:
        if conn is not None:
            conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one settlement preset in isolation")
    parser.add_argument("--case", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--hold-ms", type=int, default=0)
    args = parser.parse_args(argv)

    passed, message, details = run_case(args.case, Path(args.db), hold_ms=args.hold_ms)
    print(
        json.dumps(
            {"case_id": args.case, "passed": passed, "message": message, "details": details},
            ensure_ascii=False,
        )
    )
    sys.stdout.flush()
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
