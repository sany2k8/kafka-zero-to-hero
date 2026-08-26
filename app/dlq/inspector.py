# app/dlq/inspector.py — inspect and replay the dead-letter topic (orders.dlq).
# A DLQ is just another topic: "recovery" = read it and produce it back to the source.
import json

from confluent_kafka import Consumer, Producer, TopicPartition

from app.config import kafka_config, TOPIC_DLQ, TOPIC_ORDERS

GROUP = "dlq-replayer"   # stable group; its committed offset marks how far the DLQ is "handled"
PARTITIONS = (0, 1)


def _consumer() -> Consumer:
    cfg = kafka_config()
    cfg.update({"group.id": GROUP, "enable.auto.commit": False})
    return Consumer(cfg)


def _read_backlog(consumer: Consumer) -> list:
    """Everything between the group's committed offset and the end of orders.dlq.
    Uses manual assign + watermark offsets so the read is exactly bounded."""
    parts = [TopicPartition(TOPIC_DLQ, p) for p in PARTITIONS]
    committed = {tp.partition: tp.offset for tp in consumer.committed(parts, timeout=10)}
    starts, ends = {}, {}
    for p in PARTITIONS:
        low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC_DLQ, p), timeout=10)
        c = committed.get(p, -1)
        starts[p] = c if c is not None and c >= 0 else low   # resume from committed, else earliest
        ends[p] = high                                        # high-water mark = the end
    total = sum(ends[p] - starts[p] for p in PARTITIONS)

    consumer.assign([TopicPartition(TOPIC_DLQ, p, starts[p]) for p in PARTITIONS])
    msgs: list = []
    while len(msgs) < total:
        m = consumer.poll(2.0)
        if m is None:
            break                 # caught up (or broker slow) — stop
        if m.error():
            continue
        msgs.append(m)
    return msgs


def _decode(m) -> dict:
    return {
        "partition": m.partition(),
        "offset": m.offset(),
        "key": m.key().decode() if m.key() else None,
        "value": json.loads(m.value()),
    }


def inspect() -> dict:
    """Peek at parked messages WITHOUT consuming them — repeatable."""
    consumer = _consumer()
    try:
        msgs = _read_backlog(consumer)
        return {"count": len(msgs), "messages": [_decode(m) for m in msgs]}
    finally:
        consumer.close()          # never commit -> backlog unchanged for the next peek


def replay() -> dict:
    """Re-publish parked messages back to orders.created, then commit so they are
    marked handled and won't be replayed again."""
    consumer = _consumer()
    producer = Producer(kafka_config())
    try:
        msgs = _read_backlog(consumer)
        for m in msgs:
            producer.produce(TOPIC_ORDERS, key=m.key(), value=m.value())
        producer.flush()
        if msgs:
            consumer.commit(asynchronous=False)   # advance committed offset past the backlog
        return {"replayed": len(msgs), "to_topic": TOPIC_ORDERS}
    finally:
        consumer.close()
