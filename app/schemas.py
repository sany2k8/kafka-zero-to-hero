# app/schemas.py — the shape of the event flowing through the topic.
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone


@dataclass
class OrderCreated:
    order_id: str
    user_id: str
    items: list
    amount: float
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_bytes(self) -> bytes:                      # producer: object -> bytes
        return json.dumps(asdict(self)).encode()

    @staticmethod
    def from_bytes(raw: bytes) -> "OrderCreated":     # consumer: bytes -> object
        return OrderCreated(**json.loads(raw.decode()))
