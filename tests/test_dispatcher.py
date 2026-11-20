"""Requirement-level tests for the settlement test dispatcher."""

from __future__ import annotations

import time

import pytest

from app.db import database_fingerprint
from app.dispatcher import Dispatcher

from .conftest import wait_all_done, wait_for


def _status(state: dict, task_id: str) -> str:
    return next(t["status"] for t in state["tasks"] if t["task_id"] == task_id)


def test_audit_cases_pass_and_leave_database_untouched(dispatcher, settings, baseline):
    tasks = [dispatcher.submit(p)[0] for p in ("AUD-001", "AUD-002", "AUD-003", "AUD-004")]
    state = wait_all_done(dispatcher)
    assert all(t["result"]["passed"] for t in state["tasks"]), state
    assert len(tasks) == 4
    assert database_fingerprint(settings.db_path) == baseline


def test_refund_restores_database_after_success(dispatcher, settings, baseline):
    dispatcher.submit("REF-001")
    state = wait_all_done(dispatcher)
    task = state["tasks"][0]
    assert task["result"]["passed"], task
    assert task["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_refund_restores_database_when_case_fails_after_commit(dispatcher, settings, baseline):
    """REF-005 commits a refund then raises; the DB must still be restored."""
    dispatcher.submit("REF-005")
    state = wait_all_done(dispatcher)
    task = state["tasks"][0]
    assert task["result"]["passed"] is False
    assert "故意异常退出" in task["result"]["message"]
    assert task["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_audit_may_not_overtake_queued_refund(dispatcher):
    refund = dispatcher.submit("REF-001")[0]
    audit = dispatcher.submit("AUD-001")[0]

    seen = []
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        state = dispatcher.state()
        seen.append((_status(state, refund["task_id"]), _status(state, audit["task_id"])))
        if _status(state, audit["task_id"]) in ("done", "blocked_restore"):
            break
        time.sleep(0.02)

    # The audit must never enter running/restoring before the refund is done.
    for refund_status, audit_status in seen:
        if audit_status in ("running", "done"):
            assert refund_status == "done"
    state = wait_all_done(dispatcher)
    assert [t["preset_id"] for t in state["tasks"]] == ["REF-001", "AUD-001"]


def test_refund_waits_for_running_audits(dispatcher):
    dispatcher.extra_worker_args = ["--hold-ms", "600"]
    audit = dispatcher.submit("AUD-001")[0]
    wait_for(lambda: _status(dispatcher.state(), audit["task_id"]) == "running")
    refund = dispatcher.submit("REF-001")[0]

    overlaps = []
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        state = dispatcher.state()
        if _status(state, refund["task_id"]) == "running":
            overlaps.append(state["running_audits"])
        if _status(state, refund["task_id"]) in ("done", "blocked_restore"):
            break
        time.sleep(0.02)
    assert overlaps, "refund never ran"
    assert all(count == 0 for count in overlaps), overlaps
    dispatcher.extra_worker_args = []


def test_audit_concurrency_capped_at_two(dispatcher):
    dispatcher.extra_worker_args = ["--hold-ms", "500"]
    tasks = [dispatcher.submit("AUD-001")[0] for _ in range(4)]
    observed = 0
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        state = dispatcher.state()
        observed = max(observed, state["running_audits"])
        if all(_status(state, t["task_id"]) == "done" for t in tasks):
            break
        time.sleep(0.03)
    state = wait_all_done(dispatcher)
    assert observed == 2, f"expected exactly 2 concurrent audits, observed {observed}"
    assert state["counts"]["passed"] == 4


def test_restore_failure_pauses_dispatch_and_retry_resumes_queue_in_order(dispatcher, settings, baseline):
    dispatcher.arm_simulated_restore_failure(1)
    refund = dispatcher.submit("REF-002")[0]
    audit = dispatcher.submit("AUD-003")[0]

    state = wait_for(
        lambda: (
            s
            if (s := dispatcher.state())["paused"]
            else None
        ),
        timeout=30,
    )
    assert state["blocked_task_id"] == refund["task_id"]
    assert state["pause_reason"].startswith("数据库恢复失败")
    assert _status(state, audit["task_id"]) == "queued"
    assert state["tasks"][0]["result"]["passed"] is True  # the case itself passed
    assert state["tasks"][0]["restore"]["ok"] is False

    # While paused, new submissions are accepted but not dispatched.
    late = dispatcher.submit("AUD-004")[0]
    time.sleep(0.8)
    paused_state = dispatcher.state()
    assert _status(paused_state, audit["task_id"]) == "queued"
    assert _status(paused_state, late["task_id"]) == "queued"
    assert _status(paused_state, refund["task_id"]) == "blocked_restore"
    # The failed restore left the database dirty until the retry succeeds.
    assert database_fingerprint(settings.db_path) != baseline

    dispatcher.retry_restore()
    state = wait_all_done(dispatcher, timeout=60)
    assert state["paused"] is False
    assert state["pause_reason"] is None
    assert state["counts"]["passed"] == 3
    assert [t["preset_id"] for t in state["tasks"]] == ["REF-002", "AUD-003", "AUD-004"]
    assert database_fingerprint(settings.db_path) == baseline


def test_failed_retry_keeps_paused(dispatcher, monkeypatch):
    dispatcher.arm_simulated_restore_failure(1)
    dispatcher.submit("REF-001")
    wait_for(lambda: dispatcher.state()["paused"], timeout=30)

    original_restore = dispatcher.seed.restore

    def boom():
        raise OSError("磁盘不可用")

    monkeypatch.setattr(dispatcher.seed, "restore", boom)
    dispatcher.retry_restore()
    state = wait_for(
        lambda: (
            s
            if (s := dispatcher.state())["paused"] and not s["retrying_restore"]
            else None
        ),
        timeout=20,
    )
    assert "磁盘不可用" in state["pause_reason"]
    assert state["blocked_task_id"] is not None

    monkeypatch.setattr(dispatcher.seed, "restore", original_restore)
    dispatcher.retry_restore()
    state = wait_for(lambda: (s if not (s := dispatcher.state())["paused"] else None), timeout=30)
    assert state["tasks"][0]["restore"]["ok"] is True
    # initial attempt + failed retry + successful retry
    assert state["tasks"][0]["restore"]["attempts"] == 3


def test_unknown_preset_rejected(dispatcher):
    from app.dispatcher import UnknownPresetError

    with pytest.raises(UnknownPresetError):
        dispatcher.submit("NOPE-999")


def test_simulated_failure_arms_only_once(dispatcher):
    dispatcher.arm_simulated_restore_failure(1)
    dispatcher.submit("REF-001")
    wait_for(lambda: dispatcher.state()["paused"], timeout=30)
    dispatcher.retry_restore()
    wait_all_done(dispatcher)
    assert dispatcher.state()["simulated_restore_failures"] == 0

    dispatcher.submit("REF-002")
    state = wait_all_done(dispatcher)
    assert state["paused"] is False
    assert state["tasks"][-1]["restore"]["ok"] is True


def test_dispatcher_uses_separate_os_processes(dispatcher):
    dispatcher.submit("AUD-001")
    state = wait_all_done(dispatcher)
    task = state["tasks"][0]
    assert task["pid"] and task["pid"] > 0
    assert task["result"]["pid"] == task["pid"]
    assert "app.worker" in task["result"]["command"]


def test_next_case_dispatched_only_after_restore_completes(dispatcher):
    original = dispatcher.seed.restore
    events: list[tuple[str, float]] = []

    def slow_restore():
        events.append(("restore_start", time.time()))
        time.sleep(0.6)
        original()
        events.append(("restore_end", time.time()))

    dispatcher.seed.restore = slow_restore
    dispatcher.submit("REF-001")
    dispatcher.submit("AUD-002")
    state = wait_all_done(dispatcher)

    audit = next(t for t in state["tasks"] if t["preset_id"] == "AUD-002")
    restore_end = next(ts for name, ts in events if name == "restore_end")
    assert audit["started_at"] is not None
    assert audit["started_at"] >= restore_end, (audit["started_at"], restore_end)
    assert state["tasks"][0]["restore"]["ok"] is True


def test_audit_cannot_write(settings, baseline):
    """Audit workers open SQLite read-only; a write must fail, not corrupt data."""

    from app import worker

    original = worker.EXECUTORS["AUD-001"]

    def sneaky(conn, hold_ms=0):
        conn.execute("UPDATE orders SET amount_cents = 1 WHERE order_id = 'ORD-1001'")
        return original(conn, hold_ms=hold_ms)

    worker.EXECUTORS["AUD-001"] = sneaky
    try:
        passed, message, _ = worker.run_case("AUD-001", settings.db_path)
    finally:
        worker.EXECUTORS["AUD-001"] = original

    assert passed is False
    assert "readonly" in message.lower(), message
    assert database_fingerprint(settings.db_path) == baseline


def test_audits_do_not_run_while_refund_active(dispatcher):
    dispatcher.extra_worker_args = ["--hold-ms", "600"]
    refund = dispatcher.submit("REF-001")[0]
    wait_for(lambda: _status(dispatcher.state(), refund["task_id"]) == "running")
    audit = dispatcher.submit("AUD-001")[0]

    overlaps = []
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        state = dispatcher.state()
        if _status(state, audit["task_id"]) == "running":
            overlaps.append(state["active_refund_task_id"])
        if _status(state, audit["task_id"]) in ("done", "blocked_restore"):
            break
        time.sleep(0.02)
    state = wait_all_done(dispatcher)
    assert overlaps, "audit never ran"
    assert all(refund_id is None for refund_id in overlaps), overlaps
    assert state["counts"]["passed"] == 2


def test_timeout_kills_child_and_still_restores(settings, baseline):
    from app.config import Settings

    short = Settings(
        data_dir=settings.data_dir,
        db_path=settings.db_path,
        seed_snapshot_path=settings.seed_snapshot_path,
        max_audit_concurrency=2,
        child_timeout_seconds=1,
    )
    instance = Dispatcher(short, extra_worker_args=["--hold-ms", "5000"])
    instance.start()
    try:
        instance.submit("REF-001")

        def finished() -> dict | None:
            current = instance.state()
            busy = any(t["status"] in ("queued", "running", "restoring") for t in current["tasks"])
            return None if busy or not current["tasks"] else current

        state = wait_for(finished, timeout=30)
    finally:
        instance.stop()

    task = state["tasks"][0]
    assert task["result"]["timed_out"] is True
    assert "超时" in task["result"]["message"]
    assert task["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_startup_restores_seed_after_unclean_shutdown(settings, baseline):
    from app.db import SeedManager

    seed = SeedManager(settings)
    seed.bootstrap()
    # Simulate a crash that left a refund committed in the live database.
    import sqlite3

    conn = sqlite3.connect(settings.db_path)
    conn.execute(
        "INSERT INTO ledger_entries (order_id, kind, amount_cents, created_at)"
        " VALUES ('ORD-1001', 'refund', -100, '2026-02-01T00:00:00Z')"
    )
    conn.commit()
    conn.close()
    assert database_fingerprint(settings.db_path) != baseline

    instance = Dispatcher(settings)
    instance.start()
    try:
        assert database_fingerprint(settings.db_path) == baseline
    finally:
        instance.stop()
