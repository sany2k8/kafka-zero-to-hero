# app/eventdriven/api.py — the event-driven ingress: POST /orders -> publish OrderCreated.
# Run just this feature:  uv run uvicorn app.eventdriven.api:app --reload
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from app.kafka.producer import OrderProducer
from app.schemas import OrderCreated

router = APIRouter(tags=["event-driven"])
producer = OrderProducer()


def start() -> None:
    pass                       # the producer connects lazily on first publish


def stop() -> None:
    producer.close()           # flush buffered messages on shutdown


class OrderIn(BaseModel):
    order_id: str
    user_id: str
    items: list
    amount: float


@router.post("/orders")
def create_order(order: OrderIn):
    producer.publish(OrderCreated(**order.model_dump()))
    return {"status": "accepted", "order_id": order.order_id}   # async: work happens later


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        start()
        yield
        stop()

    app = FastAPI(title="kafka-zero-to-hero · event-driven", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
