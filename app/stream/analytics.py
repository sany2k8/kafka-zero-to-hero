# app/stream/analytics.py — a stream processor that folds orders.created into a
# materialized view (the KStream -> KTable idea), queried live by the API.
import threading
import uuid
from collections import defaultdict

from confluent_kafka import Consumer

from app.config import kafka_config, TOPIC_ORDERS
from app.schemas import OrderCreated


class OrderAnalytics:
    """Running aggregates over the whole order stream. Built once from the log,
    then kept up to date as new orders arrive. This is the 'table' half of the
    stream<->table duality; the endpoints query it (interactive queries)."""

    def __init__(self):
        self._lock = threading.Lock()          # endpoints read while the thread writes
        self.total_orders = 0
        self.total_revenue = 0.0
        self.revenue_by_user: dict[str, float] = defaultdict(float)
        self.orders_by_user: dict[str, int] = defaultdict(int)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # --- the fold: every event advances the aggregate ---
    def _apply(self, order: OrderCreated) -> None:
        with self._lock:
            self.total_orders += 1
            self.total_revenue += order.amount
            self.revenue_by_user[order.user_id] += order.amount
            self.orders_by_user[order.user_id] += 1

    # --- interactive queries: read the current state of the table ---
    def snapshot(self) -> dict:
        with self._lock:
            return {
                "total_orders": self.total_orders,
                "total_revenue": round(self.total_revenue, 2),
                "by_user": {
                    u: {"orders": self.orders_by_user[u],
                        "revenue": round(self.revenue_by_user[u], 2)}
                    for u in self.orders_by_user
                },
            }

    def user_snapshot(self, user_id: str) -> dict:
        with self._lock:
            return {
                "user_id": user_id,
                "orders": self.orders_by_user.get(user_id, 0),
                "revenue": round(self.revenue_by_user.get(user_id, 0.0), 2),
            }

    # --- the continuous consume loop, run in a background thread ---
    def _run(self) -> None:
        cfg = kafka_config()
        cfg.update({
            "group.id": f"stream-analytics-{uuid.uuid4()}",  # fresh group -> replay the WHOLE topic
            "auto.offset.reset": "earliest",                 # a table = fold over the entire log
            "enable.auto.commit": False,                     # never commit; always rebuild from start
        })
        consumer = Consumer(cfg)
        consumer.subscribe([TOPIC_ORDERS])
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None or msg.error():
                    continue
                self._apply(OrderCreated.from_bytes(msg.value()))
        finally:
            consumer.close()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
