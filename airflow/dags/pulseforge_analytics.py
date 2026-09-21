"""Finite analytics orchestration; Spark streaming is intentionally outside this DAG."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT_PROJECT_DIR = "/opt/pulseforge/analytics"
DBT_PROFILES_DIR = "/opt/pulseforge/analytics"

with DAG(
    dag_id="pulseforge_analytics",
    description="Build and verify the PulseForge PostgreSQL analytics layer",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "pulseforge",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["pulseforge", "analytics", "dbt"],
) as dag:
    verify_warehouse = BashOperator(
        task_id="verify_warehouse",
        bash_command=(
            f"dbt debug --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} --target dev"
        ),
        execution_timeout=timedelta(minutes=2),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "python -m pulseforge.product.cli pipeline "
            "--build-key '{{ dag_run.run_id }}' "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    quality_summary = BashOperator(
        task_id="quality_summary",
        bash_command=(
            "python /opt/pulseforge/scripts/summarize_dbt_run.py "
            f"{DBT_PROJECT_DIR}/target/run_results.json"
        ),
        retries=0,
        execution_timeout=timedelta(minutes=1),
    )

    verify_warehouse >> dbt_build >> quality_summary
