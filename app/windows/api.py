# app/windows/api.py — interactive queries onto tumbling-window aggregates.
# Run just this feature:  uv run uvicorn app.windows.api:app --reload
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI

from app.windows.processor import WindowedAnalytics

router = APIRouter(prefix="/windows", tags=["windows"])
windows = WindowedAnalytics(window_seconds=60, alert_threshold=3)


def start() -> None:
    windows.start()            # begin bucketing orders.created by event-time


def stop() -> None:
    windows.stop()


@router.get("/orders")
def by_window():
    # order count + revenue per tumbling window
    return windows.snapshot()


@router.get("/alerts")
def alerts():
    # velocity signal: users at/over the per-window threshold
    return windows.alerts()


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        start()
        yield
        stop()

    app = FastAPI(title="kafka-zero-to-hero · windows", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
