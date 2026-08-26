# app/services/payment.py — GROUP "payment". Run TWO copies to watch rebalancing.
#   uv run python -m app.services.payment
import random

from app.kafka.consumer import run_consumer


def process_payment(order, msg):
    print(f"[payment] charge {order.amount} for {order.order_id} "
          f"(p{msg.partition()} offset {msg.offset()})")
    if order.amount <= 0:                         # deterministic poison -> always ends up in the DLQ
        raise RuntimeError("invalid amount")
    if random.random() < 0.2:                     # simulate a flaky gateway (transient)
        raise RuntimeError("payment gateway timeout")


if __name__ == "__main__":
    run_consumer(group_id="payment", handle=process_payment)
