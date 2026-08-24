# app/api.py — turns an HTTP POST into a Kafka event, then returns fast.
#   uv run uvicorn app.api:app --reload
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from app.kafka.producer import OrderProducer
from app.schemas import OrderCreated
from app.stream.analytics import OrderAnalytics
from app.eventsourcing.events import OrderEvent, CREATED, PAID, SHIPPED, CANCELLED
from app.eventsourcing.store import EventStore

producer = OrderProducer()
analytics = OrderAnalytics()               # materialized view fed by a background stream processor
store = EventStore()                       # event-sourcing append log + projection


@asynccontextmanager
async def lifespan(app: FastAPI):
    analytics.start()                      # begin folding orders.created into the view
    store.start()                          # begin replaying orders.events into the projection
    yield
    store.stop()
    analytics.stop()
    producer.close()                       # flush on shutdown


app = FastAPI(lifespan=lifespan)


class OrderIn(BaseModel):
    order_id: str
    user_id: str
    items: list
    amount: float


@app.post("/orders")
def create_order(order: OrderIn):
    producer.publish(OrderCreated(**order.model_dump()))
    return {"status": "accepted", "order_id": order.order_id}  # async: work happens later


# --- STREAMS: interactive queries onto the materialized view ---

@app.get("/analytics/revenue")
def revenue():
    # the whole 'table': running totals folded from every order event
    return analytics.snapshot()


@app.get("/analytics/revenue/{user_id}")
def revenue_for(user_id: str):
    # point lookup by key — like querying a KTable for one row
    return analytics.user_snapshot(user_id)


# --- EVENT SOURCING: commands append events; state/history are derived by folding ---

class CreateCmd(BaseModel):
    order_id: str
    user_id: str
    items: list
    amount: float


@app.post("/es/orders")
def es_create(cmd: CreateCmd):
    store.append(OrderEvent(cmd.order_id, CREATED,
                            {"user_id": cmd.user_id, "items": cmd.items, "amount": cmd.amount}))
    return {"appended": CREATED, "order_id": cmd.order_id}


@app.post("/es/orders/{order_id}/pay")
def es_pay(order_id: str, amount: float | None = None):
    store.append(OrderEvent(order_id, PAID, {"amount": amount}))
    return {"appended": PAID, "order_id": order_id}


@app.post("/es/orders/{order_id}/ship")
def es_ship(order_id: str, tracking: str | None = None):
    store.append(OrderEvent(order_id, SHIPPED, {"tracking": tracking}))
    return {"appended": SHIPPED, "order_id": order_id}


@app.post("/es/orders/{order_id}/cancel")
def es_cancel(order_id: str, reason: str | None = None):
    store.append(OrderEvent(order_id, CANCELLED, {"reason": reason}))
    return {"appended": CANCELLED, "order_id": order_id}


@app.get("/es/orders/{order_id}")
def es_state(order_id: str):
    # current state = fold of this order's entire event history
    return store.state(order_id)


@app.get("/es/orders/{order_id}/history")
def es_history(order_id: str):
    # the audit trail — every fact that ever happened, in order
    return {"order_id": order_id, "events": store.history(order_id)}
