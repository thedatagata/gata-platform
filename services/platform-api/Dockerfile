FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ git && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install ALL deps from root pyproject (includes dbt, dlt, faker, polars, etc.)
COPY pyproject.toml uv.lock ./
COPY services/platform-api/pyproject.toml services/platform-api/pyproject.toml
COPY services/mock-data-engine/pyproject.toml services/mock-data-engine/pyproject.toml
RUN uv pip install --system --no-cache -r pyproject.toml

# App code + config files
COPY tenants.yaml supported_connectors.yaml ./
COPY services/platform-api/ services/platform-api/

# Onboarding pipeline dependencies
COPY scripts/ scripts/
COPY services/mock-data-engine/ services/mock-data-engine/
COPY warehouse/gata_transformation/ warehouse/gata_transformation/

# dbt needs profiles.yml in ~/.dbt/
RUN mkdir -p /root/.dbt && cp /app/warehouse/gata_transformation/profiles.yml /root/.dbt/profiles.yml

WORKDIR /app/services/platform-api
EXPOSE 8001
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/health')"
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
