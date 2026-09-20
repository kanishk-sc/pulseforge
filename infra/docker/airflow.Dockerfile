ARG AIRFLOW_VERSION=3.3.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION
ARG DBT_CORE_VERSION=1.12.2
ARG DBT_POSTGRES_VERSION=1.11.0

RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        "dbt-core==${DBT_CORE_VERSION}" \
        "dbt-postgres==${DBT_POSTGRES_VERSION}"

USER root
RUN mkdir -p /opt/airflow/state \
    && chown airflow:root /opt/airflow/state \
    && chmod 0770 /opt/airflow/state
USER airflow

COPY --chown=airflow:root airflow/dags /opt/airflow/dags
COPY --chown=airflow:root analytics /opt/pulseforge/analytics
COPY --chown=airflow:root scripts/summarize_dbt_run.py /opt/pulseforge/scripts/summarize_dbt_run.py
COPY --chown=airflow:root scripts/verify_airflow_dag.py /opt/pulseforge/scripts/verify_airflow_dag.py

ENV DBT_PROFILES_DIR=/opt/pulseforge/analytics
