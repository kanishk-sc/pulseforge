import base64
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from pulseforge.product.models import BuildContext

METRIC_TABLES = {
    "payments": ("product.payment_health_hourly", "metric_hour_utc"),
    "revenue": ("product.revenue_hourly", "metric_hour_utc"),
    "shipments": ("product.shipment_health_hourly", "metric_hour_utc"),
    "refunds": ("product.refund_requests_hourly", "metric_hour_utc"),
    "operations": ("product.operations_health_hourly", "metric_hour_utc"),
}


def build_context(row: dict[str, Any], stale_after_seconds: int) -> BuildContext:
    age = max(0, int((datetime.now(UTC) - row["published_at"]).total_seconds()))
    return BuildContext(
        **row,
        age_seconds=age,
        state="stale" if age > stale_after_seconds else "fresh",
    )


async def latest_build(engine: AsyncEngine, stale_after_seconds: int) -> BuildContext | None:
    query = text(
        """SELECT build_id, dbt_invocation_id, published_at, source_max_event_ts,
                  source_max_ingested_at, source_event_count
           FROM product.analytics_builds WHERE status='succeeded'
           ORDER BY published_at DESC, build_id DESC LIMIT 1"""
    )
    async with engine.connect() as connection:
        result = await connection.execute(query)
        row = result.mappings().one_or_none()
    return build_context(dict(row), stale_after_seconds) if row else None


async def metric_rows(
    engine: AsyncEngine,
    metric: str,
    build_id: UUID,
    start: datetime,
    end: datetime,
    region: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    table, time_column = METRIC_TABLES[metric]
    region_clause = "AND region_code=:region" if region else ""
    query = text(
        f"""SELECT * FROM {table}
            WHERE build_id=:build_id AND {time_column} >= :start AND {time_column} < :end
            {region_clause}
            ORDER BY {time_column}, region_code LIMIT :limit"""
    )
    async with engine.connect() as connection:
        rows = await connection.execute(
            query,
            {"build_id": build_id, "start": start, "end": end, "region": region, "limit": limit},
        )
        return [dict(row) for row in rows.mappings().all()]


async def quality_state(engine: AsyncEngine, stale_after_seconds: int) -> dict[str, Any]:
    build = await latest_build(engine, stale_after_seconds)
    query = text(
        """SELECT
          max(started_at) FILTER (WHERE status='running') AS running_build_started_at,
          max(completed_at) FILTER (WHERE status='failed') AS latest_failed_build_at,
          (array_agg(failure_reason ORDER BY completed_at DESC)
             FILTER (WHERE status='failed'))[1] AS latest_failure_reason
        FROM product.analytics_builds"""
    )
    async with engine.connect() as connection:
        state = dict((await connection.execute(query)).mappings().one())
    state["build"] = build
    state["state"] = build.state if build else "unavailable"
    state["note"] = (
        "Serving the latest successful immutable publication; running and failed builds never "
        "replace it."
        if build
        else "No successful analytics publication is available."
    )
    return state


def encode_cursor(detected_at: datetime, incident_id: UUID) -> str:
    raw = f"{detected_at.isoformat()}|{incident_id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        timestamp, incident_id = raw.split("|", 1)
        return datetime.fromisoformat(timestamp), UUID(incident_id)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("invalid cursor") from exc


async def list_incidents(
    engine: AsyncEngine,
    region: str | None,
    status: str | None,
    limit: int,
    cursor: str | None,
) -> tuple[list[dict[str, Any]], str | None]:
    filters = []
    params: dict[str, Any] = {"limit": limit + 1}
    if region:
        filters.append("region_code=:region")
        params["region"] = region
    if status:
        filters.append("status=:status")
        params["status"] = status
    if cursor:
        detected_at, incident_id = decode_cursor(cursor)
        filters.append("(detected_at, incident_id) < (:detected_at, :incident_id)")
        params.update(detected_at=detected_at, incident_id=incident_id)
    where = "WHERE " + " AND ".join(filters) if filters else ""
    query = text(
        "SELECT incident_id, detector_name, detector_version, region_code, evaluation_start, "
        "evaluation_end, observed_metric, observed_denominator, baseline_metric, threshold, "
        "severity, summary, analytics_build_id, detected_at, status, resolved_at "
        f"FROM product.incidents {where} "
        "ORDER BY detected_at DESC, incident_id DESC LIMIT :limit"
    )
    async with engine.connect() as connection:
        result = await connection.execute(query, params)
        rows = [dict(row) for row in result.mappings().all()]
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_cursor = encode_cursor(last["detected_at"], last["incident_id"])
    return rows, next_cursor


async def incident_detail(engine: AsyncEngine, incident_id: UUID) -> dict[str, Any] | None:
    async with engine.connect() as connection:
        incident = (
            (
                await connection.execute(
                    text(
                        """SELECT incident_id, detector_name, detector_version, region_code,
                                  evaluation_start, evaluation_end, observed_metric,
                                  observed_denominator, baseline_metric, threshold, severity,
                                  summary, analytics_build_id, detected_at, status, resolved_at
                           FROM product.incidents WHERE incident_id=:incident_id"""
                    ),
                    {"incident_id": incident_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if incident is None:
            return None
        evidence = await connection.execute(
            text(
                """SELECT source_event_id, evidence_role, event_ts
                   FROM product.incident_evidence WHERE incident_id=:incident_id
                   ORDER BY event_ts, source_event_id LIMIT 100"""
            ),
            {"incident_id": incident_id},
        )
    result = dict(incident)
    result["evidence"] = [dict(row) for row in evidence.mappings().all()]
    return result
