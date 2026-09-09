FROM python:3.12-slim-bookworm

ARG DBT_CORE_VERSION=1.12.2
ARG DBT_POSTGRES_VERSION=1.11.0

RUN pip install --no-cache-dir \
        "dbt-core==${DBT_CORE_VERSION}" \
        "dbt-postgres==${DBT_POSTGRES_VERSION}" \
    && useradd --uid 10002 --create-home dbt

WORKDIR /app/analytics
COPY analytics /app/analytics
USER 10002

ENV DBT_PROFILES_DIR=/app/analytics
ENTRYPOINT ["dbt"]
CMD ["build"]
