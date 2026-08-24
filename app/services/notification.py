# app/services/notification.py — GROUP "notification": emails the user.
#   uv run python -m app.services.notification
from app.kafka.consumer import run_consumer


def send_email(order, msg):
    print(f"[notification] email {order.user_id} about {order.order_id}")


if __name__ == "__main__":
    run_consumer(group_id="notification", handle=send_email)
