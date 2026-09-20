import asyncio
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


class RevenuePoint(BaseModel):
    metric_hour: datetime
    successful_payment_count: int
    successful_revenue: float


class PaymentPoint(BaseModel):
    metric_hour: datetime
    payment_attempt_count: int
    failed_payment_count: int
    payment_failure_rate: float


class ShipmentPoint(BaseModel):
    metric_hour: datetime
    shipment_created_count: int
    shipment_delay_signal_count: int


class RefundPoint(BaseModel):
    metric_hour: datetime
    refund_request_count: int
    requested_refund_amount: float


class OperationsPoint(BaseModel):
    metric_hour: datetime
    event_count: int
    average_processing_latency_ms: float | None
    warehouse_updated_at: datetime


class QualityPoint(BaseModel):
    metric_hour: datetime
    orphan_payment_attempt_count: int
    orphan_shipment_event_count: int


class AnalyticsOverview(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    hours: int
    revenue: list[RevenuePoint]
    payments: list[PaymentPoint]
    shipments: list[ShipmentPoint]
    refunds: list[RefundPoint]
    operations: list[OperationsPoint]
    quality: list[QualityPoint]


QUERIES = {
    "revenue": """
        SELECT metric_hour,
               SUM(successful_payment_count)::integer AS successful_payment_count,
               COALESCE(SUM(successful_revenue), 0)::double precision AS successful_revenue
        FROM analytics_dbt.mart_revenue_hourly
        WHERE metric_hour >= CURRENT_TIMESTAMP - (:hours * INTERVAL '1 hour')
        GROUP BY metric_hour ORDER BY metric_hour
    """,
    "payments": """
        SELECT metric_hour,
               SUM(payment_attempt_count)::integer AS payment_attempt_count,
               SUM(failed_payment_count)::integer AS failed_payment_count,
               COALESCE(SUM(failed_payment_count)::double precision
                   / NULLIF(SUM(payment_attempt_count), 0), 0) AS payment_failure_rate
        FROM analytics_dbt.mart_payment_failure_hourly
        WHERE metric_hour >= CURRENT_TIMESTAMP - (:hours * INTERVAL '1 hour')
        GROUP BY metric_hour ORDER BY metric_hour
    """,
    "shipments": """
        SELECT metric_hour,
               SUM(shipment_created_count)::integer AS shipment_created_count,
               SUM(shipment_delay_signal_count)::integer AS shipment_delay_signal_count
        FROM analytics_dbt.mart_shipment_performance_hourly
        WHERE metric_hour >= CURRENT_TIMESTAMP - (:hours * INTERVAL '1 hour')
        GROUP BY metric_hour ORDER BY metric_hour
    """,
    "refunds": """
        SELECT metric_hour,
               SUM(refund_request_count)::integer AS refund_request_count,
               COALESCE(SUM(requested_refund_amount), 0)::double precision
                   AS requested_refund_amount
        FROM analytics_dbt.mart_refunds_hourly
        WHERE metric_hour >= CURRENT_TIMESTAMP - (:hours * INTERVAL '1 hour')
        GROUP BY metric_hour ORDER BY metric_hour
    """,
    "operations": """
        SELECT metric_hour,
               SUM(event_count)::integer AS event_count,
               AVG(average_processing_latency_ms)::double precision
                   AS average_processing_latency_ms,
               MAX(warehouse_updated_at) AS warehouse_updated_at
        FROM analytics_dbt.mart_operational_health_hourly
        WHERE metric_hour >= CURRENT_TIMESTAMP - (:hours * INTERVAL '1 hour')
        GROUP BY metric_hour ORDER BY metric_hour
    """,
    "quality": """
        SELECT metric_hour,
               SUM(orphan_payment_attempt_count)::integer
                   AS orphan_payment_attempt_count,
               SUM(orphan_shipment_event_count)::integer
                   AS orphan_shipment_event_count
        FROM analytics_dbt.mart_data_quality_hourly
        WHERE metric_hour >= CURRENT_TIMESTAMP - (:hours * INTERVAL '1 hour')
        GROUP BY metric_hour ORDER BY metric_hour
    """,
}


async def _fetch_rows(engine: AsyncEngine, query: str, hours: int) -> list[dict[str, Any]]:
    async with engine.connect() as connection:
        result = await connection.execute(text(query), {"hours": hours})
        return [dict(row) for row in result.mappings().all()]


async def fetch_overview(engine: AsyncEngine, hours: int) -> AnalyticsOverview:
    names = list(QUERIES)
    results = await asyncio.gather(*(_fetch_rows(engine, QUERIES[name], hours) for name in names))
    rows = dict(zip(names, results, strict=True))
    return AnalyticsOverview(hours=hours, **rows)


class PipelineStatus(BaseModel):
    status: str
    warehouse_updated_at: datetime | None
    minutes_since_update: float | None


async def fetch_pipeline_status(engine: AsyncEngine) -> PipelineStatus:
    query = text(
        """
        SELECT MAX(warehouse_updated_at) AS warehouse_updated_at
        FROM analytics_dbt.mart_operational_health_hourly
        """
    )
    async with engine.connect() as connection:
        updated_at = (await connection.execute(query)).scalar_one_or_none()
    if updated_at is None:
        return PipelineStatus(status="empty", warehouse_updated_at=None, minutes_since_update=None)
    minutes = (datetime.now(UTC) - updated_at).total_seconds() / 60
    return PipelineStatus(
        status="fresh" if minutes <= 30 else "stale",
        warehouse_updated_at=updated_at,
        minutes_since_update=round(minutes, 2),
    )
