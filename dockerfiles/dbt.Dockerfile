FROM python:3.11-slim
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-dbt
LABEL org.opencontainers.image.description="Image with the latest dbt-core Python package including the DuckDB adapter."
ARG DBT_VERSION=""
ENV DBT_VERSION=${DBT_VERSION}
RUN apt-get update && apt-get upgrade -y && apt-get install -y git && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir kestra "dbt-core==1.11.*" "dbt-bigquery==${DBT_VERSION:-1.11.*}" "dbt-snowflake==${DBT_VERSION:-1.11.*}" "dbt-duckdb==${DBT_VERSION:-1.10.*}"
# dbt-snowflake 1.11 caps certifi<2025.4.26 and its connector caps filelock<4, both vulnerable: drop once those caps are lifted.
RUN pip install --no-cache-dir --no-deps --upgrade certifi filelock
