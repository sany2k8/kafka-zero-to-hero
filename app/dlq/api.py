# app/dlq/api.py — inspect and replay the dead-letter topic.
# Run just this feature:  uv run uvicorn app.dlq.api:app --reload
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI

from app.dlq.inspector import inspect, replay

router = APIRouter(prefix="/dlq", tags=["dlq"])


def start() -> None:
    pass                       # DLQ inspection is on-demand; no background consumer needed


def stop() -> None:
    pass


@router.get("")
def peek():
    # look at parked messages without consuming them (repeatable)
    return inspect()


@router.post("/replay")
def replay_all():
    # re-publish the backlog to orders.created, then commit so it's marked handled
    return replay()


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        start()
        yield
        stop()

    app = FastAPI(title="kafka-zero-to-hero · dlq", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
