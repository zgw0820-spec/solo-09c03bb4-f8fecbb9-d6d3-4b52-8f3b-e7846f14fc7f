"""Shared fixtures: isolated data directory and a dispatcher factory."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import Settings
from app.db import SeedManager, database_fingerprint  # noqa: E402
from app.dispatcher import Dispatcher  # noqa: E402


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    data_dir = tmp_path / "data"
    return Settings(
        data_dir=data_dir,
        db_path=data_dir / "settlement.db",
        seed_snapshot_path=data_dir / "settlement.seed.db",
        max_audit_concurrency=2,
        child_timeout_seconds=30,
    )


@pytest.fixture()
def dispatcher(settings: Settings):
    instance = Dispatcher(settings)
    instance.start()
    try:
        yield instance
    finally:
        instance.stop()


@pytest.fixture()
def baseline(settings: Settings) -> dict:
    SeedManager(settings).bootstrap()
    return database_fingerprint(settings.db_path)


def wait_for(predicate, timeout: float = 20.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition not met within timeout")


def wait_all_done(dispatcher: Dispatcher, timeout: float = 60.0) -> dict:
    def finished() -> dict | None:
        state = dispatcher.state()
        busy = any(t["status"] in ("queued", "running", "restoring") for t in state["tasks"])
        return None if busy or not state["tasks"] else state

    return wait_for(finished, timeout=timeout)
