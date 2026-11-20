"""HTTP-level tests driving the real FastAPI app in-process."""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app import main
from app.config import Settings, load_settings
from app.dispatcher import Dispatcher


@pytest.fixture()
def client(tmp_path, monkeypatch):
    settings = Settings(
        data_dir=tmp_path / "data",
        db_path=tmp_path / "data" / "settlement.db",
        seed_snapshot_path=tmp_path / "data" / "settlement.seed.db",
        max_audit_concurrency=2,
        child_timeout_seconds=30,
    )
    instance = Dispatcher(settings)
    monkeypatch.setattr(main, "dispatcher", instance)
    with TestClient(main.app) as test_client:
        yield test_client, instance


def _wait(client, predicate, timeout=30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = client.get("/api/state").json()
        if predicate(payload):
            return payload
        time.sleep(0.05)
    raise AssertionError("timeout waiting for state")


def test_index_and_presets_render(client):
    test_client, _ = client
    page = test_client.get("/")
    assert page.status_code == 200
    assert "门店结算测试调度台" in page.text
    assert "REF-001" in page.text

    presets = test_client.get("/api/presets").json()["presets"]
    assert len(presets) == 9
    kinds = {p["kind"] for p in presets}
    assert kinds == {"audit", "refund"}


def test_submit_runs_case_and_reports_result(client):
    test_client, _ = client
    response = test_client.post("/api/cases", json={"preset_id": "REF-001", "copies": 1})
    assert response.status_code == 202
    task_id = response.json()["accepted"][0]["task_id"]

    state = _wait(
        test_client,
        lambda s: any(t["task_id"] == task_id and t["status"] == "done" for t in s["tasks"]),
    )
    task = next(t for t in state["tasks"] if t["task_id"] == task_id)
    assert task["result"]["passed"] is True
    assert task["restore"]["ok"] is True
    assert task["pid"]


def test_unknown_preset_returns_404(client):
    test_client, _ = client
    response = test_client.post("/api/cases", json={"preset_id": "NOPE"})
    assert response.status_code == 404


def test_simulate_restore_failure_then_retry_via_http(client):
    test_client, _ = client
    assert test_client.post("/api/restore/simulate-failure", json={"count": 1}).json() == {
        "armed_failures": 1
    }
    test_client.post("/api/cases", json={"preset_id": "REF-002"})
    test_client.post("/api/cases", json={"preset_id": "AUD-001"})

    paused = _wait(test_client, lambda s: s["paused"])
    assert "恢复失败" in paused["pause_reason"]
    assert paused["blocked_task_id"]

    # Hard failure (not simulated) keeps the dispatcher paused and reports it.
    blocked = next(t for t in paused["tasks"] if t["task_id"] == paused["blocked_task_id"])
    assert blocked["restore"]["simulated"] is True

    accepted = test_client.post("/api/cases", json={"preset_id": "AUD-004"})
    assert accepted.status_code == 202

    retry = test_client.post("/api/restore/retry")
    assert retry.status_code == 202
    final = _wait(
        test_client,
        lambda s: not s["paused"]
        and all(t["status"] in ("done", "blocked_restore") for t in s["tasks"]),
        timeout=60,
    )
    assert final["counts"]["passed"] == 3
    assert [t["preset_id"] for t in final["tasks"]] == ["REF-002", "AUD-001", "AUD-004"]


def test_retry_without_pause_returns_409(client):
    test_client, _ = client
    response = test_client.post("/api/restore/retry")
    assert response.status_code == 409


def test_healthz(client):
    test_client, _ = client
    assert test_client.get("/healthz").json() == {"ok": True}
