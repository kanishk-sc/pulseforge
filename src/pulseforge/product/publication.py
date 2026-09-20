import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import psycopg

from pulseforge.product.schema import apply_migrations

SNAPSHOTS = {
    "payment_health_hourly": """
        SELECT payment_hour_utc, region_code, payment_attempt_count,
               successful_payment_count, failed_payment_count, failure_rate,
               attempted_amount, successful_payment_amount, failed_payment_amount
        FROM analytics_marts.mart_payment_health_hourly
    """,
    "revenue_hourly": """
        SELECT revenue_hour_utc, region_code, successful_payment_count, revenue_amount
        FROM analytics_marts.mart_revenue_hourly
    """,
    "shipment_health_hourly": """
        SELECT shipment_creation_hour_utc, region_code, shipment_created_count,
               delayed_shipment_count, shipment_delay_event_count, delayed_shipment_rate
        FROM analytics_marts.mart_shipment_health_hourly
    """,
    "refund_requests_hourly": """
        SELECT refund_request_hour_utc, region_code, refund_request_count, requested_amount
        FROM analytics_marts.mart_refunds_hourly
    """,
    "operations_health_hourly": """
        SELECT metric_hour_utc, region_code, order_count, successful_payment_count,
               revenue_amount, payment_attempt_count, failed_payment_count,
               payment_failure_rate, shipment_created_count, delayed_shipment_count,
               shipment_delay_event_count, delayed_shipment_rate, refund_request_count,
               refund_requested_amount, refund_requests_per_order
        FROM analytics_marts.mart_operations_health_hourly
    """,
}


def successful_build_for_key(connection: psycopg.Connection, build_key: str) -> UUID | None:
    row = connection.execute(
        """SELECT build_id FROM product.analytics_builds
           WHERE build_key=%s AND status='succeeded'""",
        (build_key,),
    ).fetchone()
    return row[0] if row else None


def begin_build(connection: psycopg.Connection, build_key: str) -> UUID:
    apply_migrations(connection)
    existing = connection.execute(
        "SELECT build_id, status FROM product.analytics_builds WHERE build_key=%s", (build_key,)
    ).fetchone()
    if existing:
        if existing[1] == "succeeded":
            raise ValueError(f"build_key {build_key!r} already succeeded")
        connection.execute(
            """UPDATE product.analytics_builds
               SET status='running', started_at=now(), completed_at=NULL, published_at=NULL,
                   dbt_invocation_id=NULL, failure_reason=NULL WHERE build_id=%s""",
            (existing[0],),
        )
        connection.commit()
        return existing[0]
    build_id = uuid4()
    connection.execute(
        """INSERT INTO product.analytics_builds(build_id, build_key, status, started_at)
           VALUES (%s,%s,'running',%s)""",
        (build_id, build_key, datetime.now(UTC)),
    )
    connection.commit()
    return build_id


def fail_build(connection: psycopg.Connection, build_id: UUID, reason: str) -> None:
    connection.execute(
        """UPDATE product.analytics_builds SET status='failed', completed_at=now(),
           failure_reason=%s WHERE build_id=%s AND status='running'""",
        (reason[:1000], build_id),
    )
    connection.commit()


def _artifact_metadata(run_results: Path) -> tuple[str, str]:
    content = run_results.read_bytes()
    data = json.loads(content)
    invocation_id = data.get("metadata", {}).get("invocation_id")
    if not invocation_id:
        raise ValueError("run_results.json has no dbt invocation_id")
    failed = [
        result
        for result in data.get("results", [])
        if result.get("status") not in {"success", "pass"}
    ]
    if failed:
        raise ValueError("dbt run_results contains non-success results")
    return invocation_id, hashlib.sha256(content).hexdigest()


def publish_build(connection: psycopg.Connection, build_id: UUID, run_results: str | Path) -> UUID:
    invocation_id, artifact_hash = _artifact_metadata(Path(run_results))
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        row = connection.execute(
            "SELECT status FROM product.analytics_builds WHERE build_id=%s FOR UPDATE", (build_id,)
        ).fetchone()
        if row is None or row[0] != "running":
            raise ValueError("analytics build is not in running state")
        source = connection.execute(
            """SELECT max(event_ts), max(ingested_at), count(*) FROM stream_events"""
        ).fetchone()
        for table, select_sql in SNAPSHOTS.items():
            connection.execute(f"DELETE FROM product.{table} WHERE build_id=%s", (build_id,))
            connection.execute(
                f"INSERT INTO product.{table} SELECT %s, snapshot.* FROM ({select_sql}) snapshot",
                (build_id,),
            )
        connection.execute("DELETE FROM product.analytics_evidence WHERE build_id=%s", (build_id,))
        connection.execute(
            """INSERT INTO product.analytics_evidence(
                   build_id, source_event_id, evidence_kind, event_ts, region_code)
               SELECT %s, source_event_id, 'payment_attempt', payment_event_ts, region_code
               FROM analytics_core.fct_payment_attempts
               UNION ALL
               SELECT %s, source_event_id, 'shipment_cohort', shipment_created_at, region_code
               FROM analytics_core.fct_shipments
               UNION ALL
               SELECT %s, source_event_id, 'refund_request', refund_requested_at, region_code
               FROM analytics_core.fct_refund_requests
               UNION ALL
               SELECT %s, source_event_id, 'order', order_created_at, region_code
               FROM analytics_core.fct_orders""",
            (build_id, build_id, build_id, build_id),
        )
        connection.execute(
            """UPDATE product.analytics_builds
               SET status='succeeded', completed_at=now(), published_at=now(),
                   dbt_invocation_id=%s, manifest_sha256=%s,
                   source_max_event_ts=%s, source_max_ingested_at=%s, source_event_count=%s,
                   failure_reason=NULL
               WHERE build_id=%s""",
            (invocation_id, artifact_hash, *source, build_id),
        )
    return build_id
