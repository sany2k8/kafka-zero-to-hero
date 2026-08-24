# app/eventsourcing/api.py — commands append events; state/history are derived by folding.
# Run just this feature:  uv run uvicorn app.eventsourcing.api:app --reload
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from app.eventsourcing.events import OrderEvent, CREATED, PAID, SHIPPED, CANCELLED
from app.eventsourcing.store import EventStore

router = APIRouter(prefix="/es", tags=["event-sourcing"])
store = EventStore()


def start() -> None:
    store.start()              # replay orders.events into the projection


def stop() -> None:
    store.stop()


class CreateCmd(BaseModel):
    order_id: str
    user_id: str
    items: list
    amount: float


@router.post("/orders")
def es_create(cmd: CreateCmd):
    store.append(OrderEvent(cmd.order_id, CREATED,
                            {"user_id": cmd.user_id, "items": cmd.items, "amount": cmd.amount}))
    return {"appended": CREATED, "order_id": cmd.order_id}


@router.post("/orders/{order_id}/pay")
def es_pay(order_id: str, amount: float | None = None):
    store.append(OrderEvent(order_id, PAID, {"amount": amount}))
    return {"appended": PAID, "order_id": order_id}


@router.post("/orders/{order_id}/ship")
def es_ship(order_id: str, tracking: str | None = None):
    store.append(OrderEvent(order_id, SHIPPED, {"tracking": tracking}))
    return {"appended": SHIPPED, "order_id": order_id}


@router.post("/orders/{order_id}/cancel")
def es_cancel(order_id: str, reason: str | None = None):
    store.append(OrderEvent(order_id, CANCELLED, {"reason": reason}))
    return {"appended": CANCELLED, "order_id": order_id}


@router.get("/orders/{order_id}")
def es_state(order_id: str):
    # current state = fold of this order's entire event history
    return store.state(order_id)


@router.get("/orders/{order_id}/history")
def es_history(order_id: str):
    # the audit trail — every fact that ever happened, in order
    return {"order_id": order_id, "events": store.history(order_id)}


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        start()
        yield
        stop()

    app = FastAPI(title="kafka-zero-to-hero · event-sourcing", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
