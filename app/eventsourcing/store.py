# app/eventsourcing/store.py — append-only event store on Kafka + an in-memory
# projection (read model) rebuilt by replaying the whole log.
import threading
import uuid
from collections import defaultdict

from confluent_kafka import Consumer, Producer

from app.config import kafka_config, TOPIC_EVENTS
from app.eventsourcing.events import OrderEvent, rebuild


class EventStore:
    def __init__(self):
        self._producer = Producer(kafka_config())
        self._lock = threading.Lock()
        self._events: dict[str, list[OrderEvent]] = defaultdict(list)  # order_id -> ordered events
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # --- WRITE model: append one immutable event (never update/delete) ---
    def append(self, event: OrderEvent) -> None:
        self._producer.produce(
            topic=TOPIC_EVENTS,
            key=event.order_id.encode(),   # same order -> same partition -> ordered history
            value=event.to_bytes(),
        )
        self._producer.flush()

    # --- READ model: query the projection ---
    def history(self, order_id: str) -> list[dict]:
        with self._lock:
            return [{"type": e.type, "at": e.at, "data": e.data}
                    for e in self._events.get(order_id, [])]

    def state(self, order_id: str) -> dict:
        with self._lock:
            events = list(self._events.get(order_id, []))
        return rebuild(events)             # fold outside the lock

    # --- projection builder: replay ALL events into memory ---
    def _run(self) -> None:
        cfg = kafka_config()
        cfg.update({
            "group.id": f"es-projection-{uuid.uuid4()}",  # fresh group -> replay the whole log
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer = Consumer(cfg)
        consumer.subscribe([TOPIC_EVENTS])
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None or msg.error():
                    continue
                e = OrderEvent.from_bytes(msg.value())
                with self._lock:
                    self._events[e.order_id].append(e)
        finally:
            consumer.close()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._producer.flush()
