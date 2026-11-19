# Distributor: one container per feed. Only the ProjectX feed exists today.
#
# Normally started by docker-compose.yaml in the farmer repo, on a Docker network
# with redis_host: "redis". The docker run below is for standalone runs on the host network.
#
# Build:
#   docker build -t distributor .
#
# Run (config.yaml holds credentials, so it's mounted at runtime, never baked in):
#   docker run -d --name distributor --network host \
#     -v "$PWD/config.yaml:/app/config.yaml:ro" \
#     --stop-timeout 30 \
#     distributor --config config.yaml --level info
#
# --network host lets "localhost" in config.yaml reach Redis on the host, as it does
# outside Docker. docker stop sends SIGINT (see STOPSIGNAL), which the subscriber
# handles like Ctrl+C: it unsubscribes, stops both hubs and closes Redis.

# --- Build stage: dependencies into a virtualenv (git is needed for projectx-client) ---
FROM python:3.12-slim AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Dependencies only: pyproject.toml packages no code (packages = []), and
# entrypoints/ is a directory of scripts, not something to install
WORKDIR /build
COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip setuptools \
    && python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies']))" > requirements.txt \
    && pip install --no-cache-dir -r requirements.txt

# --- Runtime stage ---
FROM python:3.12-slim

COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN useradd --create-home --uid 1000 distributor
WORKDIR /app
COPY --chown=distributor:distributor . .
USER distributor

# The subscriber shuts down cleanly on KeyboardInterrupt (SIGINT), not SIGTERM
STOPSIGNAL SIGINT

ENTRYPOINT ["python", "entrypoints/projectx.py"]
CMD ["--config", "config.yaml", "--level", "info"]
