# app/api.py — composition root: all three Kafka patterns on one app.
#   uv run uvicorn app.api:app --reload
#
# Or play with any single feature in isolation:
#   uv run uvicorn app.eventdriven.api:app --reload      # POST /orders
#   uv run uvicorn app.stream.api:app --reload           # GET  /analytics/*
#   uv run uvicorn app.windows.api:app --reload          # GET  /windows/*
#   uv run uvicorn app.eventsourcing.api:app --reload    #      /es/*
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.eventdriven import api as eventdriven
from app.stream import api as streams
from app.windows import api as windows
from app.eventsourcing import api as eventsourcing

FEATURES = (eventdriven, streams, windows, eventsourcing)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for feature in FEATURES:
        feature.start()
    yield
    for feature in reversed(FEATURES):
        feature.stop()


app = FastAPI(title="kafka-zero-to-hero", lifespan=lifespan)
for feature in FEATURES:
    app.include_router(feature.router)
