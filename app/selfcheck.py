"""Reproducible self-check for the refund write verification.

Runs the real dispatcher against a throw-away data directory and exercises the
scenarios from the requirement:

1. a clean refund case passes verification;
2. the demo preset writes another order, changes it back, commits, and fails
   verification even though the refund itself succeeded;
3. a child that commits a stray write and then crashes is still attributed to
   the case (evidence captured before the restore);
4. a child that dies mid-transaction leaves nothing to count (rollback).

Usage::

    python -m app.selfcheck            # prints JSON, exit 0 when all pass

Also runs inside the container: ``docker compose run --rm verification-selfcheck``.
"""

from __future__ import annotations

import json
import sys
import tempfile
import time
from pathlib import Path

from app.config import Settings
from app.dispatcher import Dispatcher


def _wait(dispatcher: Dispatcher, timeout: float = 90.0) -> list[dict]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = dispatcher.state()
        busy = any(t["status"] in ("queued", "running", "restoring") for t in state["tasks"])
        if not busy and state["tasks"]:
            return state["tasks"]
        time.sleep(0.05)
    raise TimeoutError("自检等待用例完成超时")


def run() -> int:
    with tempfile.TemporaryDirectory(prefix="settle-selfcheck-") as tmp:
        data = Path(tmp) / "data"
        settings = Settings(
            data_dir=data,
            db_path=data / "settlement.db",
            seed_snapshot_path=data / "settlement.seed.db",
        )
        dispatcher = Dispatcher(settings)
        dispatcher.start()
        try:
            dispatcher.submit("REF-001")
            dispatcher.submit("REF-006")
            dispatcher.submit_selftest(
                "SELFTEST-STRAY-COMMIT-CRASH", ["ORD-1001"], "SELFTEST-COMMIT-CRASH"
            )
            dispatcher.submit_selftest(
                "SELFTEST-STRAY-ROLLBACK-CRASH", ["ORD-1001"], "SELFTEST-ROLLBACK-CRASH"
            )
            tasks = _wait(dispatcher)
            matches_seed = dispatcher.seed.matches_seed()
        finally:
            dispatcher.stop()

    results = {task["preset_id"]: task for task in tasks}
    checks: list[dict] = []

    def check(name: str, ok: bool, detail: str) -> None:
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    clean = results["REF-001"]
    check(
        "干净退款核验通过",
        clean["result"]["passed"] and clean["verification"]["status"] == "passed",
        f"verify={clean['verification']['status']} passed={clean['result']['passed']}",
    )

    demo = results["REF-006"]
    check(
        "误写另一订单后改回并提交仍判失败",
        demo["result"]["worker_passed"]
        and not demo["result"]["passed"]
        and demo["verification"]["status"] == "failed"
        and demo["verification"]["violating_order_ids"] == ["ORD-1004"],
        f"worker_passed={demo['result']['worker_passed']} "
        f"passed={demo['result']['passed']} "
        f"tables={demo['verification']['violating_tables']} "
        f"orders={demo['verification']['violating_order_ids']}",
    )
    check(
        "演示用例复位成功",
        demo["restore"]["ok"] is True,
        f"restore_ok={demo['restore']['ok']}",
    )

    crash = results["SELFTEST-COMMIT-CRASH"]
    check(
        "提交后崩溃仍取证并归到本次用例",
        not crash["result"]["passed"]
        and crash["verification"]["status"] == "failed"
        and crash["verification"]["task_id"] == crash["task_id"]
        and crash["verification"]["violating_order_ids"] == ["ORD-1004"],
        f"verify={crash['verification']['status']} "
        f"evidence_task={crash['verification']['task_id'][:8]} "
        f"exit={crash['result']['exit_code']}",
    )
    check(
        "提交后崩溃仍复位成功",
        crash["restore"]["ok"] is True,
        f"restore_ok={crash['restore']['ok']}",
    )

    rollback = results["SELFTEST-ROLLBACK-CRASH"]
    check(
        "事务中崩溃的回滚写入不计",
        not rollback["result"]["passed"]
        and rollback["verification"]["status"] == "passed"
        and rollback["verification"]["write_count"] == 0,
        f"verify={rollback['verification']['status']} "
        f"writes={rollback['verification']['write_count']}",
    )

    check("全部用例结束后库与起始状态一致", matches_seed, f"matches_seed={matches_seed}")

    payload = {
        "ok": all(item["ok"] for item in checks),
        "checks": checks,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    sys.exit(run())
