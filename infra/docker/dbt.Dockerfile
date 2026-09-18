FROM python:3.12-slim-bookworm

ARG DBT_POSTGRES_VERSION=1.11.0
RUN pip install --no-cache-dir "dbt-postgres==${DBT_POSTGRES_VERSION}"

WORKDIR /workspace
ENTRYPOINT ["dbt"]
