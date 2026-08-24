# app/services/inventory.py — GROUP "inventory": reserves stock.
#   uv run python -m app.services.inventory
from app.kafka.consumer import run_consumer


def reserve_stock(order, msg):
    print(f"[inventory] reserve {len(order.items)} items for {order.order_id}")


if __name__ == "__main__":
    run_consumer(group_id="inventory", handle=reserve_stock)
