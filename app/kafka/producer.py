# app/kafka/producer.py — wraps the Producer; the ONLY writer to Kafka.
from confluent_kafka import Producer

from app.config import kafka_config, TOPIC_ORDERS
from app.schemas import OrderCreated


class OrderProducer:
    def __init__(self):
        self._p = Producer(kafka_config())

    def _on_delivery(self, err, msg):
        # librdkafka calls this once the broker ACKs (or fails) the record.
        if err:
            print(f"DELIVERY FAILED: {err}")
        else:
            print(f"delivered -> {msg.topic()} p{msg.partition()} offset {msg.offset()}")

    def publish(self, order: OrderCreated):
        self._p.produce(
            topic=TOPIC_ORDERS,
            key=order.order_id.encode(),   # KEY -> which partition -> ordering guarantee
            value=order.to_bytes(),
            callback=self._on_delivery,
        )
        self._p.poll(0)                    # serve delivery callbacks, non-blocking

    def close(self):
        self._p.flush()                    # block until all buffered records are delivered
