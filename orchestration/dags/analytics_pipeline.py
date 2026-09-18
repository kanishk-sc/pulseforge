from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT = "cd /opt/pulseforge/dbt && dbt"

with DAG(
    dag_id="pulseforge_analytics_pipeline",
    description="Validate stream freshness, build warehouse models, aggregate marts and test them.",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["pulseforge", "analytics"],
) as dag:
    verify_ingestion_freshness = BashOperator(
        task_id="verify_ingestion_freshness",
        bash_command=f"{DBT} source freshness --profiles-dir . --target dev",
    )
    build_staging_dimensions_facts = BashOperator(
        task_id="build_staging_dimensions_facts",
        bash_command=(
            f"{DBT} run --profiles-dir . --target dev "
            "--select path:models/staging path:models/dimensions path:models/facts"
        ),
    )
    aggregate_business_marts = BashOperator(
        task_id="aggregate_business_marts",
        bash_command=f"{DBT} run --profiles-dir . --target dev --select path:models/marts",
    )
    test_warehouse = BashOperator(
        task_id="test_warehouse",
        bash_command=f"{DBT} test --profiles-dir . --target dev",
    )

    (
        verify_ingestion_freshness
        >> build_staging_dimensions_facts
        >> aggregate_business_marts
        >> test_warehouse
    )
