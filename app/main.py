"""FastAPI web application for the settlement test dispatcher."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app.cases import PRESETS
from app.config import load_settings
from app.dispatcher import Dispatcher, QueueFullError, UnknownPresetError

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

settings = load_settings()
dispatcher = Dispatcher(settings)


@asynccontextmanager
async def lifespan(_: FastAPI):
    dispatcher.start()
    try:
        yield
    finally:
        dispatcher.stop()


app = FastAPI(title="门店结算测试调度台", lifespan=lifespan)


class SubmitRequest(BaseModel):
    preset_id: str = Field(min_length=1, max_length=64)
    copies: int = Field(default=1, ge=1, le=20)


class SimulateRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=10)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"presets": [preset.to_dict() for preset in PRESETS]},
    )


@app.get("/api/presets")
async def list_presets() -> dict:
    return {"presets": [preset.to_dict() for preset in PRESETS]}


@app.get("/api/state")
async def get_state() -> dict:
    return await asyncio.to_thread(dispatcher.state)


@app.post("/api/cases", status_code=202)
async def submit_case(payload: SubmitRequest) -> dict:
    try:
        tasks = await asyncio.to_thread(dispatcher.submit, payload.preset_id, payload.copies)
    except UnknownPresetError:
        raise HTTPException(status_code=404, detail=f"未知预置用例: {payload.preset_id}")
    except QueueFullError as exc:
        raise HTTPException(status_code=429, detail=str(exc))
    return {"accepted": tasks}


@app.post("/api/restore/retry", status_code=202)
async def retry_restore() -> dict:
    try:
        return await asyncio.to_thread(dispatcher.retry_restore)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@app.get("/api/db/fingerprint")
async def db_fingerprint() -> dict:
    """Read-only view of the live test database, useful for manual verification."""
    return await asyncio.to_thread(dispatcher.seed.read_fingerprint)


@app.post("/api/restore/simulate-failure")
async def simulate_failure(payload: SimulateRequest | None = None) -> dict:
    count = payload.count if payload else 1
    armed = await asyncio.to_thread(dispatcher.arm_simulated_restore_failure, count)
    return {"armed_failures": armed}


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({"ok": True})
