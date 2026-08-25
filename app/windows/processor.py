# app/windows/processor.py — tumbling-window aggregates over orders.created,
# bucketed by EVENT time (the order's created_at), not when we consume it.
import threading
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer

from app.config import kafka_config, TOPIC_ORDERS
from app.schemas import OrderCreated


class WindowedAnalytics:
    def __init__(self, window_seconds: int = 60, alert_threshold: int = 3):
        self.window_seconds = window_seconds        # tumbling window size
        self.alert_threshold = alert_threshold      # orders/user/window that trip an alert
        self._lock = threading.Lock()
        self._windows: dict[int, dict] = {}         # window_start (epoch) -> aggregates
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _bucket(self, iso_ts: str) -> int:
        # EVENT-TIME windowing: floor the order's own timestamp to its window.
        # A late/out-of-order event still maps to its correct window here.
        t = datetime.fromisoformat(iso_ts).timestamp()
        return int(t // self.window_seconds) * self.window_seconds

    def _apply(self, order: OrderCreated) -> None:
        start = self._bucket(order.created_at)
        with self._lock:
            w = self._windows.get(start)
            if w is None:
                w = {"orders": 0, "revenue": 0.0, "by_user": defaultdict(int)}
                self._windows[start] = w
            w["orders"] += 1
            w["revenue"] += order.amount
            w["by_user"][order.user_id] += 1

    @staticmethod
    def _iso(epoch: int) -> str:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

    def snapshot(self) -> dict:
        with self._lock:
            windows = [
                {"window_start": self._iso(start),
                 "orders": w["orders"],
                 "revenue": round(w["revenue"], 2)}
                for start, w in sorted(self._windows.items())
            ]
        return {"window_seconds": self.window_seconds, "windows": windows}

    def alerts(self) -> dict:
        with self._lock:
            alerts = [
                {"window_start": self._iso(start), "user_id": u, "orders": c}
                for start, w in sorted(self._windows.items())
                for u, c in w["by_user"].items()
                if c >= self.alert_threshold
            ]
        return {"window_seconds": self.window_seconds,
                "threshold": self.alert_threshold,
                "alerts": alerts}

    def _run(self) -> None:
        cfg = kafka_config()
        cfg.update({
            "group.id": f"windowed-analytics-{uuid.uuid4()}",  # fresh group -> replay whole log
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
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
