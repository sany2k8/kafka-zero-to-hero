# app/kafka/admin.py — run ONCE to create topics with the partition count we want.
#   uv run python -m app.kafka.admin
from confluent_kafka.admin import AdminClient, NewTopic

from app.config import kafka_config, TOPIC_ORDERS, TOPIC_DLQ


def create_topics():
    admin = AdminClient(kafka_config())
    topics = [
        NewTopic(TOPIC_ORDERS, num_partitions=2, replication_factor=2),  # 2 brokers in this cluster
        NewTopic(TOPIC_DLQ,    num_partitions=2, replication_factor=2),
    ]
    for name, fut in admin.create_topics(topics).items():
        try:
            fut.result()                       # block until the broker confirms
            print(f"created {name}")
        except Exception as e:
            print(f"{name}: {e}")              # "already exists" is harmless


if __name__ == "__main__":
    create_topics()
