# Single image for all three roles (naming server, storage server, client).
# The role is selected by the command in docker-compose.yml.
FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and generate the gRPC stubs (they are gitignored).
COPY proto/ proto/
COPY scripts/ scripts/
COPY gfs/ gfs/
RUN python scripts/gen_proto.py

ENV PYTHONUNBUFFERED=1

# Default to the naming server; compose overrides per service.
CMD ["python", "-m", "gfs.naming_server"]
