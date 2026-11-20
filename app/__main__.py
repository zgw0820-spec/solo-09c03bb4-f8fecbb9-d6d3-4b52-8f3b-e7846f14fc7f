"""Module entry point so ``python -m app`` runs the server."""

from __future__ import annotations

import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=os.environ.get("SETTLE_HOST", "0.0.0.0"),
        port=int(os.environ.get("SETTLE_PORT", "8000")),
        log_level=os.environ.get("SETTLE_LOG_LEVEL", "info"),
    )
