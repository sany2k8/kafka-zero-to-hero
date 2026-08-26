# syntax=docker/dockerfile:1
# ──────────────────────────────────────────────────────────────────────────────
# Stage 1 – build: install dependencies with uv into an isolated venv
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.13-slim AS builder

# Install uv (fast Python package manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency manifests first (layer-cache friendly)
COPY pyproject.toml uv.lock ./

# Sync dependencies into .venv (no editable install of the project itself yet)
RUN uv sync --frozen --no-install-project

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2 – runtime: lean image with only the venv + application code
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.13-slim AS runtime

# Non-root user for security
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

# Copy the pre-built venv from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy application source
COPY app/ ./app/

# Put venv binaries first in PATH so uvicorn / python resolve from there
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
