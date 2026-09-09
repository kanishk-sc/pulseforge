"""Fail CI when the analytics DAG cannot import or its finite task graph drifts."""

import json
from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

DAG_DIRECTORY = Path("/opt/airflow/dags")
EXPECTED_TASKS = {"verify_warehouse", "dbt_build", "quality_summary"}


def main() -> None:
    dag_bag = DagBag(dag_folder=str(DAG_DIRECTORY), safe_mode=False)
    if dag_bag.import_errors:
        raise SystemExit(json.dumps(dag_bag.import_errors, sort_keys=True))
    dag = dag_bag.dags.get("pulseforge_analytics")
    if dag is None:
        raise SystemExit("pulseforge_analytics DAG was not discovered")
    if set(dag.task_ids) != EXPECTED_TASKS:
        raise SystemExit(f"unexpected task set: {sorted(dag.task_ids)}")
    if dag.catchup or dag.max_active_runs != 1:
        raise SystemExit("analytics DAG must disable catchup and serialize active runs")
    if dag.task_dict["verify_warehouse"].downstream_task_ids != {"dbt_build"}:
        raise SystemExit("verify_warehouse must lead only to dbt_build")
    if dag.task_dict["dbt_build"].downstream_task_ids != {"quality_summary"}:
        raise SystemExit("dbt_build must lead only to quality_summary")
    forbidden = ("spark", "streaming", "checkpoint")
    commands = " ".join(getattr(task, "bash_command", "") for task in dag.tasks).lower()
    if any(term in commands for term in forbidden):
        raise SystemExit("DAG must not orchestrate Spark streaming or checkpoints")
    print(
        json.dumps(
            {
                "dag_id": dag.dag_id,
                "schedule": str(dag.schedule),
                "tasks": sorted(dag.task_ids),
                "import_errors": 0,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
