FROM python:3.11-slim
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-dbt
LABEL org.opencontainers.image.description="Image with the latest dbt-duckdb Python package"
ARG DBT_VERSION=""
ENV DBT_VERSION=${DBT_VERSION}
RUN apt-get update && apt-get upgrade -y && apt-get install -y git && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir kestra "dbt-core==1.11.*" "requests" "dbt-duckdb==${DBT_VERSION:-1.10.*}"
