FROM python:3.11-slim
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-dbt
LABEL org.opencontainers.image.description="Image with the latest dbt-core Python package including the DuckDB adapter."
ARG DBT_VERSION=""
ENV DBT_VERSION=${DBT_VERSION}
RUN apt-get update && apt-get install -y git && apt-get clean
RUN pip install --no-cache-dir kestra \
	dbt-bigquery${DBT_VERSION:+==$DBT_VERSION} \
	dbt-snowflake${DBT_VERSION:+==$DBT_VERSION} \
	dbt-duckdb${DBT_VERSION:+==$DBT_VERSION}