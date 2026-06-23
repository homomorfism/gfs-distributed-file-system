# Single image for all three roles (naming server, storage server, client).
# The role is selected by the command in docker-compose.yml.
FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /uvx /bin/

# Install dependencies first for better layer caching.
COPY pyproject.toml uv.lock .
RUN uv sync --locked --no-dev
ENV PATH="/app/.venv/bin:$PATH"

# Copy source and generate the gRPC stubs (they are gitignored).
COPY proto/ proto/
COPY scripts/ scripts/
COPY gfs/ gfs/
RUN uv run python scripts/gen_proto.py

ENV PYTHONUNBUFFERED=1

# Default to the naming server; compose overrides per service.
CMD ["python", "-m", "gfs.naming_server"]
