"""Best-effort finite-job telemetry persisted outside the publication transaction."""

import json
import logging
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import psycopg

from pulseforge.product.detectors import evaluate

logger = logging.getLogger(__name__)


def _record_failure(connection: psycopg.Connection, stage: str, exc: Exception) -> None:
    connection.rollback()
    logger.warning(
        "operational_telemetry_write_failed",
        extra={"stage": stage, "error_type": type(exc).__name__},
    )


def record_dbt_quality(connection: psycopg.Connection, build_id: UUID, run_results: Path) -> None:
    """The build is already published; a telemetry write may not revoke it."""
    try:
        data = json.loads(run_results.read_text(encoding="utf-8"))
        statuses = Counter(row["status"] for row in data["results"])
        if not statuses or set(statuses) - {"pass", "success"}:
            raise ValueError("dbt artifact does not contain only verified results")
        connection.execute(
            """INSERT INTO product.dbt_quality_runs
                 (build_id, result_count, pass_count, success_count)
               VALUES (%s,%s,%s,%s) ON CONFLICT (build_id) DO NOTHING""",
            (build_id, sum(statuses.values()), statuses["pass"], statuses["success"]),
        )
        connection.commit()
    except Exception as exc:
        _record_failure(connection, "dbt_quality", exc)


def recorded_evaluate(
    connection: psycopg.Connection,
    now: datetime | None,
    stale_after_seconds: int,
) -> list[UUID]:
    """Record detector attempts and skips without changing the detector's result."""
    run_id = uuid4()
    started = datetime.now(UTC)
    evaluation_at = now or started
    recorded = False
    try:
        connection.execute(
            """INSERT INTO product.detector_runs
                 (run_id,evaluation_at,started_at,status)
               VALUES (%s,%s,%s,'running')""",
            (run_id, evaluation_at, started),
        )
        connection.commit()
        recorded = True
    except Exception as exc:
        _record_failure(connection, "detector_start", exc)

    report: dict = {}
    try:
        created = evaluate(connection, now, stale_after_seconds, report=report)
        connection.commit()
    except Exception as exc:
        connection.rollback()
        if recorded:
            try:
                connection.execute(
                    """UPDATE product.detector_runs SET status='failed',
                         completed_at=now(), error_type=%s WHERE run_id=%s""",
                    (type(exc).__name__[:100], run_id),
                )
                connection.commit()
            except Exception as telemetry_exc:
                _record_failure(connection, "detector_failure", telemetry_exc)
        raise

    if recorded:
        try:
            connection.execute(
                """UPDATE product.detector_runs
                   SET status=%s, completed_at=now(), skip_reason=%s,
                       analytics_build_id=%s, created_incidents=%s
                   WHERE run_id=%s""",
                (
                    "skipped" if report.get("skip_reason") else "succeeded",
                    report.get("skip_reason"),
                    report.get("build_id"),
                    len(created),
                    run_id,
                ),
            )
            connection.commit()
        except Exception as exc:
            _record_failure(connection, "detector_finish", exc)
    return created
