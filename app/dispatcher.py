"""In-process scheduler for settlement test cases.

Scheduling rules implemented here:

* Audit cases are read-only and at most ``max_audit_concurrency`` may run at once.
* Refund cases write to the shared database and run exclusively.
* Dispatch order is strict FIFO: only the head of the queue may start, so an
  audit submitted after a queued refund can never overtake it.
* After every refund child process exits -- success, business failure, crash or
  timeout -- the database is restored from the canonical seed before the next
  item is dispatched.
* If a restore fails, the dispatcher records the error, keeps accepting new
  cases, and pauses dispatch.  A successful manual restore retry resumes the
  untouched queue in its original order.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from app.cases import PRESETS_BY_ID, CaseDefinition
from app.config import Settings
from app.db import SeedManager

Status = Literal["queued", "running", "restoring", "done", "blocked_restore"]

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class QueueFullError(Exception):
    pass


class UnknownPresetError(Exception):
    pass


@dataclass
class Task:
    task_id: str
    preset_id: str
    name: str
    kind: str
    expectation: str
    status: Status = "queued"
    submitted_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    restore: dict[str, Any] | None = None
    restore_error: str | None = None
    pid: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "preset_id": self.preset_id,
            "name": self.name,
            "kind": self.kind,
            "expectation": self.expectation,
            "status": self.status,
            "submitted_at": self.submitted_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result,
            "error": self.error,
            "restore": self.restore,
            "restore_error": self.restore_error,
            "pid": self.pid,
        }


class Dispatcher:
    def __init__(
        self,
        settings: Settings,
        seed_manager: SeedManager | None = None,
        extra_worker_args: list[str] | None = None,
    ) -> None:
        self.settings = settings
        self.seed = seed_manager or SeedManager(settings)
        self.extra_worker_args = list(extra_worker_args or [])

        self._cond = threading.Condition()
        self._queue: deque[str] = deque()
        self._tasks: dict[str, Task] = {}
        self._order: list[str] = []
        self._running_audits: set[str] = set()
        self._active_refund: str | None = None
        self._paused = False
        self._pause_reason: str | None = None
        self._blocked_task_id: str | None = None
        self._simulated_restore_failures = 0
        self._retrying = False
        self._stop = threading.Event()
        self._loop_thread: threading.Thread | None = None
        self._started = False
        self._max_history = 500

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        with self._cond:
            if self._started:
                return
            self.seed.bootstrap()
            self._started = True
            self._stop.clear()
            self._loop_thread = threading.Thread(
                target=self._dispatch_loop, name="settlement-dispatcher", daemon=True
            )
            self._loop_thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._cond:
            self._cond.notify_all()
        thread = self._loop_thread
        if thread is not None:
            thread.join(timeout=5)

    # -- public API --------------------------------------------------------

    def submit(self, preset_id: str, copies: int = 1) -> list[dict[str, Any]]:
        definition = PRESETS_BY_ID.get(preset_id)
        if definition is None:
            raise UnknownPresetError(preset_id)
        copies = max(1, min(int(copies), 20))
        with self._cond:
            queued = sum(1 for tid in self._queue if self._tasks[tid].status == "queued")
            if queued + copies > self.settings.max_queue_size:
                raise QueueFullError(f"队列已满（上限 {self.settings.max_queue_size}）")
            created: list[Task] = []
            for _ in range(copies):
                task = self._new_task(definition)
                self._tasks[task.task_id] = task
                self._order.append(task.task_id)
                self._queue.append(task.task_id)
                created.append(task)
            self._prune_history_locked()
            self._cond.notify_all()
            return [t.to_dict() for t in created]

    def arm_simulated_restore_failure(self, count: int = 1) -> int:
        with self._cond:
            self._simulated_restore_failures += max(1, min(int(count), 10))
            return self._simulated_restore_failures

    def retry_restore(self) -> dict[str, Any]:
        with self._cond:
            if not self._paused or self._blocked_task_id is None:
                raise ValueError("当前没有等待恢复的重试用例")
            if self._retrying:
                raise ValueError("恢复重试正在进行中")
            task_id = self._blocked_task_id
            self._retrying = True
            thread = threading.Thread(
                target=self._retry_restore_worker,
                args=(task_id,),
                name=f"settlement-restore-retry-{task_id[:8]}",
                daemon=True,
            )
            thread.start()
            return {"accepted": True, "blocked_task_id": task_id}

    def state(self) -> dict[str, Any]:
        with self._cond:
            tasks = [self._tasks[tid] for tid in self._order]
            queued = [t for t in tasks if t.status == "queued"]
            positions = {t.task_id: idx + 1 for idx, t in enumerate(queued)}
            payload_tasks = []
            for task in tasks:
                item = task.to_dict()
                item["queue_position"] = positions.get(task.task_id)
                # Report the child process runtime, excluding any restore wait.
                if task.result and task.result.get("duration_ms") is not None:
                    item["duration_ms"] = task.result["duration_ms"]
                elif task.started_at and task.finished_at:
                    item["duration_ms"] = int((task.finished_at - task.started_at) * 1000)
                else:
                    item["duration_ms"] = None
                payload_tasks.append(item)
            payload = {
                "paused": self._paused,
                "pause_reason": self._pause_reason,
                "blocked_task_id": self._blocked_task_id,
                "retrying_restore": self._retrying,
                "simulated_restore_failures": self._simulated_restore_failures,
                "max_audit_concurrency": self.settings.max_audit_concurrency,
                "running_audits": len(self._running_audits),
                "active_refund_task_id": self._active_refund,
                "queue_length": len(queued),
                "tasks": payload_tasks,
                "counts": {
                    "queued": len(queued),
                    "running": sum(1 for t in tasks if t.status in ("running", "restoring")),
                    "passed": sum(
                        1
                        for t in tasks
                        if t.status == "done" and t.result and t.result.get("passed")
                    ),
                    "failed": sum(
                        1
                        for t in tasks
                        if (t.status == "done" and t.result and not t.result.get("passed"))
                        or t.status == "blocked_restore"
                    ),
                },
                "db": None,
                "server_time": time.time(),
            }
        # Read the database summary outside the lock: it performs file I/O and
        # must not stall dispatch or submissions.
        payload["db"] = self._db_summary()
        return payload

    def _db_summary(self) -> dict[str, Any]:
        try:
            fingerprint = self.seed.read_fingerprint()
            return {
                "ok": True,
                "integrity": fingerprint["integrity"],
                "orders": len(fingerprint["orders"]),
                "ledger_entries": len(fingerprint["ledger_entries"]),
                "refund_requests": len(fingerprint["refund_requests"]),
                "matches_seed": fingerprint == self.seed.seed_fingerprint(),
            }
        except Exception as exc:  # noqa: BLE001 - status endpoint must not crash
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    # -- scheduler ---------------------------------------------------------

    def _new_task(self, definition: CaseDefinition) -> Task:
        return Task(
            task_id=uuid.uuid4().hex,
            preset_id=definition.preset_id,
            name=definition.name,
            kind=definition.kind,
            expectation=definition.expectation,
        )

    def _prune_history_locked(self) -> None:
        if len(self._order) <= self._max_history:
            return
        removable: list[str] = []
        for task_id in self._order:
            task = self._tasks[task_id]
            if task.status in ("done",) and len(self._order) - len(removable) > self._max_history:
                removable.append(task_id)
        for task_id in removable:
            self._order.remove(task_id)
            self._tasks.pop(task_id, None)

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            refund_task: Task | None = None
            with self._cond:
                while not self._stop.is_set():
                    if self._paused:
                        self._cond.wait(timeout=0.5)
                        continue
                    if not self._queue:
                        self._cond.wait(timeout=0.5)
                        continue
                    head = self._tasks[self._queue[0]]
                    if head.kind == "refund":
                        if self._running_audits:
                            self._cond.wait(timeout=0.5)
                            continue
                        self._queue.popleft()
                        self._begin_locked(head)
                        self._active_refund = head.task_id
                        refund_task = head
                        break
                    if len(self._running_audits) >= self.settings.max_audit_concurrency:
                        self._cond.wait(timeout=0.5)
                        continue
                    self._queue.popleft()
                    self._begin_locked(head)
                    self._running_audits.add(head.task_id)
                    thread = threading.Thread(
                        target=self._run_audit,
                        args=(head.task_id,),
                        name=f"settlement-audit-{head.task_id[:8]}",
                        daemon=True,
                    )
                    thread.start()
                    # Loop again immediately: a second audit may fit.
            if refund_task is not None:
                self._run_refund(refund_task.task_id)

    def _begin_locked(self, task: Task) -> None:
        task.status = "running"
        task.started_at = time.time()

    # -- execution ---------------------------------------------------------

    def _worker_command(self, preset_id: str) -> list[str]:
        return [
            sys.executable,
            "-m",
            "app.worker",
            "--case",
            preset_id,
            "--db",
            str(self.settings.db_path),
            *self.extra_worker_args,
        ]

    def _run_child(self, preset_id: str, task: Task | None = None) -> dict[str, Any]:
        command = self._worker_command(preset_id)
        started = time.monotonic()
        env = dict(os.environ)
        env.setdefault("PYTHONUNBUFFERED", "1")
        env["PYTHONPATH"] = os.pathsep.join(
            [str(PROJECT_ROOT), env.get("PYTHONPATH", "")]
        ).rstrip(os.pathsep)
        display = " ".join(command)
        try:
            process = subprocess.Popen(
                command,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                start_new_session=True,
            )
        except OSError as exc:
            return {
                "passed": False,
                "message": f"无法启动用例进程: {exc}",
                "details": {},
                "exit_code": None,
                "timed_out": False,
                "duration_ms": int((time.monotonic() - started) * 1000),
                "command": display,
                "pid": None,
            }

        if task is not None:
            with self._cond:
                task.pid = process.pid

        timed_out = False
        try:
            stdout, stderr = process.communicate(timeout=self.settings.child_timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, stderr = process.communicate()
        duration_ms = int((time.monotonic() - started) * 1000)
        return self._parse_child_output(
            preset_id, process.pid, process.returncode, stdout, stderr, timed_out, duration_ms, display
        )

    def _parse_child_output(
        self,
        preset_id: str,
        pid: int | None,
        exit_code: int | None,
        stdout: str,
        stderr: str,
        timed_out: bool,
        duration_ms: int,
        command: str,
    ) -> dict[str, Any]:
        report: dict[str, Any] | None = None
        for line in reversed(stdout.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                candidate = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict) and candidate.get("case_id") == preset_id:
                report = candidate
                break
        if report is None:
            message = (
                f"用例进程超时（>{self.settings.child_timeout_seconds}s），已强制结束"
                if timed_out
                else f"用例进程未返回结果（exit={exit_code}）"
            )
            if stderr.strip():
                message += f"；stderr: {stderr.strip()[-800:]}"
            return {
                "passed": False,
                "message": message,
                "details": {},
                "exit_code": exit_code,
                "timed_out": timed_out,
                "duration_ms": duration_ms,
                "command": command,
                "pid": pid,
            }
        passed = bool(report.get("passed"))
        message = report.get("message", "")
        if passed and exit_code not in (0, None):
            # A worker that reports success but exits abnormally is not trusted.
            passed = False
            message = f"用例进程异常退出（exit={exit_code}），不采信其通过结果"
        return {
            "passed": passed,
            "message": message,
            "details": report.get("details", {}),
            "exit_code": exit_code,
            "timed_out": timed_out,
            "duration_ms": duration_ms,
            "command": command,
            "pid": pid,
        }

    def _run_audit(self, task_id: str) -> None:
        with self._cond:
            task = self._tasks[task_id]
            preset_id = task.preset_id
        result = self._run_child(preset_id, task)
        with self._cond:
            task.result = result
            task.finished_at = time.time()
            task.status = "done"
            if not result.get("passed"):
                task.error = result.get("message")
            self._running_audits.discard(task_id)
            self._cond.notify_all()

    def _run_refund(self, task_id: str) -> None:
        with self._cond:
            task = self._tasks[task_id]
            preset_id = task.preset_id
        result = self._run_child(preset_id, task)

        with self._cond:
            task.result = result
            task.status = "restoring"
            if not result.get("passed"):
                task.error = result.get("message")
            self._cond.notify_all()

        # Restoration happens after the child has fully exited, regardless of
        # whether the case passed, failed, crashed or timed out.
        restore_info: dict[str, Any] = {
            "ok": False,
            "started_at": time.time(),
            "simulated": False,
            "attempts": 1,
        }
        restore_error: str | None = None
        try:
            self._restore_database(simulate_allowed=True)
            restore_info["ok"] = True
        except Exception as exc:  # noqa: BLE001 - surfaced to the UI
            restore_error = f"{type(exc).__name__}: {exc}"
            restore_info["simulated"] = isinstance(exc, SimulatedRestoreError)
        restore_info["finished_at"] = time.time()

        with self._cond:
            task.restore = restore_info
            task.finished_at = time.time()
            self._active_refund = None
            if restore_error is not None:
                task.status = "blocked_restore"
                task.restore_error = restore_error
                self._paused = True
                self._pause_reason = f"数据库恢复失败，已暂停派发：{restore_error}"
                self._blocked_task_id = task.task_id
            else:
                task.status = "done"
                task.restore_error = None
            self._cond.notify_all()

    def _restore_database(self, simulate_allowed: bool) -> None:
        with self._cond:
            if simulate_allowed and self._simulated_restore_failures > 0:
                self._simulated_restore_failures -= 1
                raise SimulatedRestoreError("模拟恢复失败（由网页触发）")
        self.seed.restore()

    def _retry_restore_worker(self, task_id: str) -> None:
        started = time.time()
        error: str | None = None
        try:
            self.seed.restore()
        except Exception as exc:  # noqa: BLE001 - stay paused on failure
            error = f"{type(exc).__name__}: {exc}"
        finished = time.time()
        with self._cond:
            task = self._tasks.get(task_id)
            self._retrying = False
            if task is not None:
                previous = task.restore if isinstance(task.restore, dict) else {}
                attempts = int(previous.get("attempts", 1)) + 1
                task.restore = {
                    **(previous or {}),
                    "ok": error is None,
                    "retry_started_at": started,
                    "retry_finished_at": finished,
                    "attempts": attempts,
                }
            if error is None:
                if task is not None:
                    task.status = "done"
                    task.restore_error = None
                self._paused = False
                self._pause_reason = None
                self._blocked_task_id = None
            else:
                self._pause_reason = f"数据库恢复重试失败，仍在暂停：{error}"
                if task is not None:
                    task.restore_error = error
            self._cond.notify_all()


class SimulatedRestoreError(RuntimeError):
    pass
