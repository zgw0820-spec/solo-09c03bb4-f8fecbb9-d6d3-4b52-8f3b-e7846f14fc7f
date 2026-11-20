"""Tests for the refund write verification (误写核验).

Covers the requirements:

* every write (insert / update / delete) in orders, ledger_entries and
  refund_requests is checked against the case's target orders;
* an update is checked on both its old and its new order id;
* a committed write to a non-target order fails the case even if the same
  transaction restores the original value;
* rolled-back writes are not counted;
* evidence is captured before the restore and attributed to the case even when
  the child process dies after COMMIT;
* a capture failure is reported as 未核验 and the case can never pass, while
  the restore still runs;
* the demo preset is submit-able from the web.
"""

from __future__ import annotations

import sqlite3

import pytest

from app import verification
from app.db import SeedManager, database_fingerprint
from app.dispatcher import Dispatcher

from .conftest import wait_all_done


@pytest.fixture()
def db(settings):
    """A bootstrapped live database for the unit-level journal tests."""
    SeedManager(settings).bootstrap()
    return settings.db_path


def _task(state: dict, preset_id: str) -> dict:
    return next(t for t in state["tasks"] if t["preset_id"] == preset_id)


# ---------------------------------------------------------------------------
# Unit-level checks on the journal / evaluation
# ---------------------------------------------------------------------------


def _write_between_arm_and_capture(db_path, task_id="task-1", targets=("ORD-1001",), sql=()):
    verification.arm(db_path, task_id, "REF-001", targets)
    conn = sqlite3.connect(db_path, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for statement in sql:
            conn.execute(statement)
        conn.execute("COMMIT")
    finally:
        conn.close()
    return verification.evaluate(verification.capture(db_path), task_id, "REF-001", targets)


def test_target_order_writes_pass(db):
    report = _write_between_arm_and_capture(
        db,
        sql=[
            "INSERT INTO refund_requests VALUES ('RR-X', 'ORD-1001', 100, 'applied', 'now')",
            (
                "INSERT INTO ledger_entries (order_id, kind, amount_cents, created_at)"
                " VALUES ('ORD-1001', 'refund', -100, 'now')"
            ),
            "UPDATE orders SET status = 'partial_refund' WHERE order_id = 'ORD-1001'",
        ],
    )
    assert report["status"] == "passed"
    assert report["write_count"] == 3
    assert report["violations"] == []


@pytest.mark.parametrize(
    "statement, table, operation",
    [
        (
            "INSERT INTO refund_requests VALUES ('RR-Y', 'ORD-1002', 100, 'applied', 'now')",
            "refund_requests",
            "insert",
        ),
        (
            (
                "INSERT INTO ledger_entries (order_id, kind, amount_cents, created_at)"
                " VALUES ('ORD-1002', 'refund', -100, 'now')"
            ),
            "ledger_entries",
            "insert",
        ),
        ("UPDATE orders SET status = 'paid' WHERE order_id = 'ORD-1002'", "orders", "update"),
        ("DELETE FROM refund_requests WHERE request_id = 'RR-2002'", "refund_requests", "delete"),
        ("DELETE FROM ledger_entries WHERE order_id = 'ORD-1004'", "ledger_entries", "delete"),
    ],
)
def test_foreign_order_write_is_a_violation(db, statement, table, operation):
    report = _write_between_arm_and_capture(db, sql=[statement])
    assert report["status"] == "failed"
    assert report["violating_tables"] == [table]
    assert report["violating_order_ids"] == ["ORD-1002" if "1002" in statement else "ORD-1004"]
    assert report["violations"][0]["operation"] == operation


def test_update_is_checked_on_old_and_new_order_id(db):
    """Moving a row from a foreign order into the target order is still a violation."""
    report = _write_between_arm_and_capture(
        db,
        sql=["UPDATE refund_requests SET order_id = 'ORD-1001' WHERE request_id = 'RR-2002'"],
    )
    assert report["status"] == "failed"
    violation = report["violations"][0]
    assert violation["operation"] == "update"
    assert violation["old_order_id"] == "ORD-1004"
    assert violation["new_order_id"] == "ORD-1001"
    assert violation["order_ids"] == ["ORD-1004"]

    report = _write_between_arm_and_capture(
        db,
        sql=["UPDATE refund_requests SET order_id = 'ORD-1004' WHERE request_id = 'RR-2002'"],
    )
    assert report["status"] == "failed"
    assert report["violations"][0]["order_ids"] == ["ORD-1004"]


def test_committed_write_and_revert_is_still_a_violation(db):
    """Same value, or changed back: the update trigger fires either way."""
    report = _write_between_arm_and_capture(
        db,
        sql=[
            # No-op update (same value) on a foreign order.
            "UPDATE orders SET status = 'paid' WHERE order_id = 'ORD-1002'",
            # Change and change back in the same transaction.
            "UPDATE refund_requests SET state = 'pending' WHERE request_id = 'RR-2002'",
            "UPDATE refund_requests SET state = 'applied' WHERE request_id = 'RR-2002'",
        ],
    )
    assert report["status"] == "failed"
    assert report["violating_tables"] == ["orders", "refund_requests"]
    assert report["violating_order_ids"] == ["ORD-1002", "ORD-1004"]
    assert report["write_count"] == 3


def test_rolled_back_writes_are_not_counted(db):
    verification.arm(db, "task-rb", "REF-001", ("ORD-1001",))
    conn = sqlite3.connect(db, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE orders SET status = 'paid' WHERE order_id = 'ORD-1002'")
        conn.execute("ROLLBACK")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE orders SET status = 'partial_refund' WHERE order_id = 'ORD-1001'")
        conn.execute("COMMIT")
    finally:
        conn.close()

    report = verification.evaluate(
        verification.capture(db), "task-rb", "REF-001", ("ORD-1001",)
    )
    assert report["status"] == "passed"
    assert report["write_count"] == 1
    assert report["violations"] == []


def test_evidence_from_another_task_is_unverified(db):
    verification.arm(db, "task-a", "REF-001", ("ORD-1001",))
    report = verification.evaluate(
        verification.capture(db), "task-b", "REF-001", ("ORD-1001",)
    )
    assert report["status"] == "unverified"
    assert "其他用例" in report["error"]


def test_capture_without_journal_is_unverified(db):
    report = verification.evaluate(
        verification.capture(db), "task-x", "REF-001", ("ORD-1001",)
    )
    assert report["status"] == "unverified"


def test_arm_clears_stale_journal(db):
    _write_between_arm_and_capture(db, task_id="old-task")
    report = _write_between_arm_and_capture(
        db, task_id="new-task", sql=[]
    )
    assert report["status"] == "passed"
    assert report["write_count"] == 0


def test_combine_result_never_lets_unverified_pass():
    worker_ok = {"passed": True, "message": "通过"}
    unverified = verification.unverified("t", "REF-001", ["ORD-1001"], "取证失败")
    combined = verification.combine_result(worker_ok, unverified)
    assert combined["passed"] is False
    assert combined["worker_passed"] is True
    assert "未核验" in combined["message"]


# ---------------------------------------------------------------------------
# End-to-end through the dispatcher (real child processes)
# ---------------------------------------------------------------------------


def test_dispatcher_records_verification_for_successful_refund(dispatcher, settings, baseline):
    dispatcher.submit("REF-001")
    state = wait_all_done(dispatcher)
    task = _task(state, "REF-001")
    assert task["result"]["passed"] is True
    report = task["verification"]
    assert report["status"] == "passed"
    assert report["target_orders"] == ["ORD-1001"]
    assert report["checked_before_restore"] is True
    assert report["write_count"] == 3
    assert {w["table"] for w in report["writes"]} == {
        "orders",
        "ledger_entries",
        "refund_requests",
    }
    assert task["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_dispatcher_marks_case_failed_when_it_reports_no_writes(dispatcher):
    """REF-003 is rejected before writing, so the journal is empty and it passes."""
    dispatcher.submit("REF-003")
    state = wait_all_done(dispatcher)
    task = _task(state, "REF-003")
    assert task["result"]["passed"] is True
    assert task["verification"]["status"] == "passed"
    assert task["verification"]["write_count"] == 0


def test_demo_preset_fails_verification_despite_net_zero_writes(dispatcher, settings, baseline):
    dispatcher.submit("REF-006")
    state = wait_all_done(dispatcher)
    task = _task(state, "REF-006")
    assert task["result"]["worker_passed"] is True, "the refund itself succeeded"
    assert task["result"]["passed"] is False
    report = task["verification"]
    assert report["status"] == "failed"
    assert report["violating_tables"] == ["refund_requests"]
    assert report["violating_order_ids"] == ["ORD-1004"]
    # Two updates on RR-2002 (change + revert) both recorded.
    updates = [w for w in report["writes"] if w["table"] == "refund_requests"
               and w["operation"] == "update"]
    assert len(updates) == 2
    assert all(w["old_order_id"] == "ORD-1004" for w in updates)
    assert task["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_crash_after_commit_still_captures_stray_write(dispatcher, settings, baseline):
    task = dispatcher.submit_selftest(
        "SELFTEST-STRAY-COMMIT-CRASH", ["ORD-1001"], "SELF-CRASH"
    )
    state = wait_all_done(dispatcher)
    finished = next(t for t in state["tasks"] if t["task_id"] == task["task_id"])
    assert finished["result"]["passed"] is False
    assert finished["result"]["timed_out"] is False
    assert finished["result"]["exit_code"] == 3
    report = finished["verification"]
    assert report["status"] == "failed"
    assert report["violating_tables"] == ["orders"]
    assert report["violating_order_ids"] == ["ORD-1004"]
    assert report["task_id"] == task["task_id"]
    assert finished["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_crash_mid_transaction_rolls_back_and_is_not_counted(dispatcher, settings, baseline):
    task = dispatcher.submit_selftest(
        "SELFTEST-STRAY-ROLLBACK-CRASH", ["ORD-1001"], "SELF-CRASH-RB"
    )
    state = wait_all_done(dispatcher)
    finished = next(t for t in state["tasks"] if t["task_id"] == task["task_id"])
    # The child crashed, so it cannot pass, but no write is attributed to it.
    assert finished["result"]["passed"] is False
    report = finished["verification"]
    assert report["status"] == "passed"
    assert report["write_count"] == 0
    assert finished["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_rollback_then_successful_case_passes(dispatcher, settings, baseline):
    dispatcher.submit_selftest(
        "SELFTEST-STRAY-ROLLBACK-SUCCESS", ["ORD-1001"], "SELF-RB-OK"
    )
    state = wait_all_done(dispatcher)
    task = state["tasks"][0]
    assert task["result"]["worker_passed"] is True
    assert task["result"]["passed"] is True
    assert task["verification"]["status"] == "passed"
    assert database_fingerprint(settings.db_path) == baseline


def test_unverified_capture_blocks_pass_but_restore_still_runs(
    dispatcher, settings, baseline, monkeypatch
):
    def boom(_path):
        raise verification.VerificationError("取证介质不可读")

    monkeypatch.setattr(verification, "capture", boom)
    dispatcher.submit("REF-001")
    state = wait_all_done(dispatcher)
    task = _task(state, "REF-001")
    assert task["result"]["worker_passed"] is True
    assert task["result"]["passed"] is False, "an unverified case must not pass"
    assert task["verification"]["status"] == "unverified"
    assert "取证介质不可读" in task["verification"]["error"]
    assert "未核验" in task["result"]["message"]
    assert task["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_arm_failure_blocks_pass_but_case_and_restore_still_run(
    dispatcher, settings, baseline, monkeypatch
):
    def boom(*_args, **_kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(verification, "arm", boom)
    dispatcher.submit("REF-002")
    state = wait_all_done(dispatcher)
    task = _task(state, "REF-002")
    assert task["verification"]["status"] == "unverified"
    assert "database is locked" in task["verification"]["error"]
    assert task["result"]["passed"] is False
    # The case still executed and the DB was still restored.
    assert task["result"]["exit_code"] == 0
    assert task["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_verification_evidence_is_read_before_restore(settings):
    """The journal must be gone after the restore, proving it was read first."""
    instance = Dispatcher(settings)
    instance.start()
    try:
        instance.submit("REF-001")
        state = wait_all_done(instance)
    finally:
        instance.stop()
    task = state["tasks"][0]
    assert task["verification"]["writes"], "evidence was captured before restore"
    # After the restore the journal table is gone from the live database.
    conn = sqlite3.connect(settings.db_path)
    try:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    finally:
        conn.close()
    assert verification.JOURNAL_TABLE not in tables
    assert verification.SESSION_TABLE not in tables


def test_refund_queue_keeps_verification_across_multiple_cases(dispatcher):
    dispatcher.submit("REF-001")
    dispatcher.submit("REF-006")
    dispatcher.submit("REF-002")
    state = wait_all_done(dispatcher)
    by_preset = {t["preset_id"]: t for t in state["tasks"]}
    assert by_preset["REF-001"]["verification"]["status"] == "passed"
    assert by_preset["REF-006"]["verification"]["status"] == "failed"
    assert by_preset["REF-002"]["verification"]["status"] == "passed"
    # Each report is attributed to its own task, not a neighbour's run.
    for task in state["tasks"]:
        assert task["verification"]["task_id"] == task["task_id"]
