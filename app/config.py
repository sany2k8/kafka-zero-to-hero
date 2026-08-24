# app/config.py — single source of truth for talking to Aiven Kafka.
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Repo root, so the CA cert is found no matter which directory you run from.
BASE_DIR = Path(__file__).resolve().parent.parent


def _ca_path() -> str:
    ca = Path(os.environ.get("KAFKA_CA", "ca.pem"))
    return str(ca if ca.is_absolute() else BASE_DIR / ca)


def kafka_config() -> dict:
    return {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],  # host:port from your snapshot
        "security.protocol": "SASL_SSL",      # encrypted transport + auth
        "sasl.mechanisms": "SCRAM-SHA-256",   # Aiven's default (confirm in console)
        "sasl.username": os.environ["KAFKA_USER"],      # avnadmin
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        "ssl.ca.location": _ca_path(),        # the CA cert you downloaded from Aiven
    }


# Topic names live here so producer, consumer, and admin never disagree.
TOPIC_ORDERS = "orders.created"
TOPIC_DLQ = "orders.dlq"
TOPIC_EVENTS = "orders.events"   # event-sourcing store: the append-only order lifecycle log
