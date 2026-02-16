FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Deps first (cache layer)
COPY pyproject.toml uv.lock ./
COPY services/platform-api/pyproject.toml services/platform-api/pyproject.toml
RUN uv pip install --system --no-cache -r services/platform-api/pyproject.toml

# App code + config files the API needs
COPY tenants.yaml supported_connectors.yaml ./
COPY services/platform-api/ services/platform-api/

WORKDIR /app/services/platform-api
EXPOSE 8001
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/health')"
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
