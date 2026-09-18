FROM apache/airflow:3.3.2-python3.12

ARG DBT_POSTGRES_VERSION=1.11.0
RUN pip install --no-cache-dir "dbt-postgres==${DBT_POSTGRES_VERSION}"
