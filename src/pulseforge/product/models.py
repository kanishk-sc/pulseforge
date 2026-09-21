from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer

Region = Literal["us-east", "us-west", "eu-west", "ap-south"]
DataState = Literal["fresh", "stale", "empty"]


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @field_serializer("*", when_used="json", check_fields=False)
    def serialize_decimal(self, value):
        if isinstance(value, Decimal):
            return format(value, "f")
        return value


class BuildContext(ApiModel):
    build_id: UUID
    dbt_invocation_id: str
    published_at: datetime
    source_max_event_ts: datetime | None
    source_max_ingested_at: datetime | None
    source_event_count: int
    age_seconds: int = Field(ge=0)
    state: DataState


class PaymentPoint(ApiModel):
    metric_hour_utc: datetime
    region_code: str
    payment_attempt_count: int
    successful_payment_count: int
    failed_payment_count: int
    failure_rate: Decimal | None
    attempted_amount: Decimal
    successful_payment_amount: Decimal
    failed_payment_amount: Decimal


class RevenuePoint(ApiModel):
    metric_hour_utc: datetime
    region_code: str
    successful_payment_count: int
    revenue_amount: Decimal


class ShipmentPoint(ApiModel):
    metric_hour_utc: datetime
    region_code: str
    shipment_created_count: int
    delayed_shipment_count: int
    shipment_delay_event_count: int
    delayed_shipment_rate: Decimal | None
    cohort_mature: bool


class RefundPoint(ApiModel):
    metric_hour_utc: datetime
    region_code: str
    refund_request_count: int
    requested_amount: Decimal


class OperationsPoint(ApiModel):
    metric_hour_utc: datetime
    region_code: str
    order_count: int
    payment_attempt_count: int
    failed_payment_count: int
    payment_failure_rate: Decimal | None
    shipment_created_count: int
    delayed_shipment_count: int
    delayed_shipment_rate: Decimal | None
    refund_request_count: int
    refund_requests_per_order: Decimal | None


class MetricResponse(ApiModel):
    state: DataState
    interval_start: datetime
    interval_end: datetime
    interval_semantics: Literal["[start,end)"] = "[start,end)"
    build: BuildContext
    metric_definition: str
    points: list[dict]


class QualityStatus(ApiModel):
    state: DataState | Literal["unavailable"]
    build: BuildContext | None
    running_build_started_at: datetime | None = None
    latest_failed_build_at: datetime | None = None
    latest_failure_reason: str | None = None
    note: str


class IncidentSummary(ApiModel):
    incident_id: UUID
    detector_name: str
    detector_version: str
    region_code: str
    evaluation_start: datetime
    evaluation_end: datetime
    observed_metric: Decimal
    observed_denominator: int | None
    baseline_metric: Decimal
    threshold: Decimal
    severity: Literal["warning", "critical"]
    summary: str
    analytics_build_id: UUID
    detected_at: datetime
    status: Literal["open", "resolved"]
    resolved_at: datetime | None


class IncidentEvidence(ApiModel):
    source_event_id: UUID
    evidence_role: str
    event_ts: datetime


class IncidentDetail(IncidentSummary):
    evidence: list[IncidentEvidence]


class IncidentList(ApiModel):
    items: list[IncidentSummary]
    next_cursor: str | None = None


class PipelineStatus(ApiModel):
    analytics: QualityStatus
    streaming_status: Literal["not_observed"] = "not_observed"
    note: str = (
        "The product service has no authoritative Spark liveness signal; only analytics "
        "publication "
        "state is reported."
    )
