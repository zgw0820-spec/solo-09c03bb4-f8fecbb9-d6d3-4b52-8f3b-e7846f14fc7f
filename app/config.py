"""Runtime configuration for the settlement test dispatcher."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # fail fast on bad configuration
        raise ValueError(f"environment variable {name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    seed_snapshot_path: Path
    max_audit_concurrency: int = 2
    child_timeout_seconds: int = 30
    max_queue_size: int = 200
    retention_seconds: int = 3600


def load_settings() -> Settings:
    data_dir = Path(os.environ.get("SETTLE_DATA_DIR", "data")).resolve()
    db_path = Path(os.environ.get("SETTLE_DB_PATH", str(data_dir / "settlement.db"))).resolve()
    snapshot = Path(
        os.environ.get("SETTLE_SNAPSHOT_PATH", str(data_dir / "settlement.seed.db"))
    ).resolve()
    return Settings(
        data_dir=data_dir,
        db_path=db_path,
        seed_snapshot_path=snapshot,
        max_audit_concurrency=max(1, _env_int("SETTLE_MAX_AUDIT", 2)),
        child_timeout_seconds=max(1, _env_int("SETTLE_CHILD_TIMEOUT", 30)),
        max_queue_size=max(1, _env_int("SETTLE_MAX_QUEUE", 200)),
        retention_seconds=max(60, _env_int("SETTLE_RETENTION", 3600)),
    )
