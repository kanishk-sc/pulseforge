import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

DBT_TARGET = Path(os.getenv("DBT_TARGET_PATH", "/tmp/dbt-target"))
ARTIFACT_ROOT = Path("/opt/airflow/artifacts/quality")


@task
def persist_quality_summary() -> str:
    """Persist a compact report from actual dbt results; never invent pass counts."""
    results = json.loads((DBT_TARGET / "run_results.json").read_text())
    counts: dict[str, int] = {}
    failures = []
    for result in results["results"]:
        status = str(result["status"])
        counts[status] = counts.get(status, 0) + 1
        if status not in {"pass", "success", "warn"}:
            failures.append(result["unique_id"])
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "elapsed_time_seconds": results.get("elapsed_time"),
        "status_counts": counts,
        "failure_ids": failures,
    }
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    destination = ARTIFACT_ROOT / f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    destination.write_text(json.dumps(report, indent=2) + "\n")
    return str(destination)


with DAG(
    dag_id="pulseforge_data_quality_report",
    description="Run dbt quality checks and persist their measured outcome as JSON.",
    schedule="5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["pulseforge", "quality"],
) as dag:
    run_quality_tests = BashOperator(
        task_id="run_quality_tests",
        bash_command=(
            "cd /opt/pulseforge/dbt && dbt test --profiles-dir . --target dev --store-failures"
        ),
    )
    run_quality_tests >> persist_quality_summary()
