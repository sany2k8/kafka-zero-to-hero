# app/stream/api.py — interactive queries onto the materialized view.
# Run just this feature:  uv run uvicorn app.stream.api:app --reload
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI

from app.stream.analytics import OrderAnalytics

router = APIRouter(prefix="/analytics", tags=["streams"])
analytics = OrderAnalytics()


def start() -> None:
    analytics.start()          # begin folding orders.created into the view


def stop() -> None:
    analytics.stop()


@router.get("/revenue")
def revenue():
    # the whole 'table': running totals folded from every order event
    return analytics.snapshot()


@router.get("/revenue/{user_id}")
def revenue_for(user_id: str):
    # point lookup by key — like querying a KTable for one row
    return analytics.user_snapshot(user_id)


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        start()
        yield
        stop()

    app = FastAPI(title="kafka-zero-to-hero · streams", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
