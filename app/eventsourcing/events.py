# app/eventsourcing/events.py — the immutable events + the reducer that folds
# them into current state. State is DERIVED here, never stored.
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone

# Event types = the order's lifecycle transitions (past tense: facts that happened).
CREATED = "OrderCreated"
PAID = "PaymentCompleted"
SHIPPED = "OrderShipped"
CANCELLED = "OrderCancelled"


@dataclass
class OrderEvent:
    order_id: str
    type: str
    data: dict
    at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @staticmethod
    def from_bytes(raw: bytes) -> "OrderEvent":
        return OrderEvent(**json.loads(raw.decode()))


def apply(state: dict, e: OrderEvent) -> dict:
    """Reducer: evolve the aggregate by exactly one event."""
    if e.type == CREATED:
        state.update(order_id=e.order_id, status="CREATED", **e.data)
    elif e.type == PAID:
        state["status"] = "PAID"
        if e.data.get("amount") is not None:
            state["paid_amount"] = e.data["amount"]
    elif e.type == SHIPPED:
        state["status"] = "SHIPPED"
        state["tracking"] = e.data.get("tracking")
    elif e.type == CANCELLED:
        state["status"] = "CANCELLED"
        state["cancel_reason"] = e.data.get("reason")
    state["version"] = state.get("version", 0) + 1   # how many events have been folded in
    return state


def rebuild(events: list[OrderEvent]) -> dict:
    """Current state = a left-fold over the ENTIRE event history of one order."""
    state: dict = {"status": "UNKNOWN", "version": 0}
    for e in events:
        apply(state, e)
    return state
