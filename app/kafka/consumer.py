# app/kafka/consumer.py — one reusable consumer loop used by every service.
from confluent_kafka import Consumer, Producer

from app.config import kafka_config, TOPIC_ORDERS, TOPIC_DLQ
from app.schemas import OrderCreated


def run_consumer(group_id: str, handle, max_retries: int = 3):
    cfg = kafka_config()
    cfg.update({
        "group.id": group_id,             # identity of the consumer GROUP
        "auto.offset.reset": "earliest",  # first run with no offset -> start from the beginning
        "enable.auto.commit": False,      # WE commit -> we control at-least-once
    })
    consumer = Consumer(cfg)
    dlq = Producer(kafka_config())
    consumer.subscribe([TOPIC_ORDERS])    # joining the group triggers a REBALANCE

    seen = set()                          # naive idempotency store (real life: Redis/DB)
    try:
        while True:
            msg = consumer.poll(1.0)      # long-poll for the next record
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            order = OrderCreated.from_bytes(msg.value())

            if order.order_id in seen:        # IDEMPOTENCY: already processed -> skip
                consumer.commit(msg)
                continue

            for attempt in range(1, max_retries + 1):   # RETRIES
                try:
                    handle(order, msg)              # the service's business logic
                    seen.add(order.order_id)
                    consumer.commit(msg)            # commit ONLY after success -> advance OFFSET
                    break
                except Exception as e:
                    print(f"[{group_id}] attempt {attempt} failed: {e}")
                    if attempt == max_retries:      # give up -> DEAD-LETTER
                        dlq.produce(TOPIC_DLQ, key=msg.key(), value=msg.value())
                        dlq.flush()
                        consumer.commit(msg)        # skip the poison message, keep flowing
    finally:
        consumer.close()                  # leave the group cleanly -> fast rebalance
