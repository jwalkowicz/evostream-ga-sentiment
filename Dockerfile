# to do this file:
# this is just an example and placeholder

FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files first for caching
COPY pyproject.toml uv.lock ./

# Install dependencies into the system environment (no virtual env needed inside Docker)
RUN uv sync --system

# Copy the rest of the application
COPY . .
