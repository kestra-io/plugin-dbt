FROM python:3.11-slim
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-dbt
LABEL org.opencontainers.image.description="Image with the latest dbt-dremio Python package"
ARG DBT_VERSION=""
ENV DBT_VERSION=${DBT_VERSION}
RUN apt-get update && apt-get install -y git && apt-get clean
RUN pip install --no-cache-dir kestra dbt-dremio${DBT_VERSION:+==$DBT_VERSION}