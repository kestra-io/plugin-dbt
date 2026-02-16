FROM python:3.11-slim
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-dbt
LABEL org.opencontainers.image.description="Image with dbt Fusion installed."
ARG DBT_VERSION=""
ENV DBT_VERSION=${DBT_VERSION}
RUN apt-get update && apt-get install -y curl git && apt-get clean

RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update --version ${DBT_VERSION}
RUN /root/.local/bin/dbt --version
ENV PATH="/root/.local/bin:${PATH}"

