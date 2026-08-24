# app/api.py — turns an HTTP POST into a Kafka event, then returns fast.
#   uv run uvicorn app.api:app --reload
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from app.kafka.producer import OrderProducer
from app.schemas import OrderCreated
from app.stream.analytics import OrderAnalytics

producer = OrderProducer()
analytics = OrderAnalytics()               # materialized view fed by a background stream processor


@asynccontextmanager
async def lifespan(app: FastAPI):
    analytics.start()                      # begin folding orders.created into the view
    yield
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
