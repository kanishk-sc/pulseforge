import asyncio
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Response
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from pulseforge.config import Settings
from pulseforge.product.models import (
    IncidentDetail,
    IncidentList,
    IncidentSummary,
    MetricResponse,
    PipelineStatus,
    QualityStatus,
    Region,
)
from pulseforge.product.store import (
    incident_detail,
    latest_build,
    list_incidents,
    metric_rows,
    quality_state,
)

DEFINITIONS = {
    "payments": "Failed payment attempts divided by all payment attempts in each UTC hour.",
    "revenue": "USD amount from successful payment events only; order amounts are excluded.",
    "shipments": (
        "Shipment creation cohorts and their later delay signals. Cohorts younger than 24 hours "
        "are marked immature."
    ),
    "refunds": "Refund requests and requested amounts; these are not completed refunds.",
    "operations": (
        "Hourly order, payment, shipment, and refund counts with explicit nullable denominators."
    ),
}


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise HTTPException(status_code=422, detail=f"{name}_must_include_timezone")
    return value.astimezone(UTC)


def _interval(
    start: datetime | None, end: datetime | None, max_days: int
) -> tuple[datetime, datetime]:
    resolved_end = _utc(end, "end") if end else datetime.now(UTC)
    resolved_start = _utc(start, "start") if start else resolved_end - timedelta(hours=24)
    if resolved_start >= resolved_end:
        raise HTTPException(status_code=422, detail="start_must_precede_end")
    if resolved_end - resolved_start > timedelta(days=max_days):
        raise HTTPException(status_code=422, detail=f"range_exceeds_{max_days}_days")
    return resolved_start, resolved_end


def create_product_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    async def metrics(
        metric: str,
        request: Request,
        response: Response,
        start: datetime | None,
        end: datetime | None,
        region: Region | None,
        limit: int,
    ) -> MetricResponse:
        resolved_start, resolved_end = _interval(start, end, settings.analytics_max_range_days)
        try:
            build = await latest_build(
                request.app.state.engine, settings.analytics_stale_after_seconds
            )
        except (SQLAlchemyError, TimeoutError) as exc:
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc
        if build is None:
            raise HTTPException(status_code=503, detail="analytics_publication_unavailable")
        cache_key = ":".join(
            (
                "product",
                "v1",
                metric,
                str(build.build_id),
                resolved_start.isoformat(),
                resolved_end.isoformat(),
                region or "all",
                str(limit),
            )
        )
        try:
            async with asyncio.timeout(0.5):
                cached = await request.app.state.redis.get(cache_key)
            if cached:
                response.headers["X-Cache"] = "hit"
                cached_result = MetricResponse.model_validate_json(cached)
                # Cache the immutable points, but recompute time-sensitive publication state.
                cached_result.build = build
                cached_result.state = "empty" if not cached_result.points else build.state
                return cached_result
        except (RedisError, TimeoutError):
            pass
        try:
            points = await metric_rows(
                request.app.state.engine,
                metric,
                build.build_id,
                resolved_start,
                resolved_end,
                region,
                limit,
            )
        except (SQLAlchemyError, TimeoutError) as exc:
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc
        for point in points:
            point.pop("build_id", None)
        if metric == "shipments":
            maturity_cutoff = datetime.now(UTC) - timedelta(hours=24)
            for point in points:
                point["cohort_mature"] = point["metric_hour_utc"] <= maturity_cutoff
        result = MetricResponse(
            state="empty" if not points else build.state,
            interval_start=resolved_start,
            interval_end=resolved_end,
            build=build,
            metric_definition=DEFINITIONS[metric],
            points=points,
        )
        try:
            async with asyncio.timeout(0.5):
                await request.app.state.redis.setex(
                    cache_key, settings.analytics_cache_ttl_seconds, result.model_dump_json()
                )
            response.headers["X-Cache"] = "miss"
        except (RedisError, TimeoutError):
            response.headers["X-Cache"] = "bypass"
        return result

    Start = Annotated[datetime | None, Query(description="Inclusive UTC interval start")]
    End = Annotated[datetime | None, Query(description="Exclusive UTC interval end")]
    RegionFilter = Annotated[Region | None, Query()]
    Limit = Annotated[int, Query(ge=1, le=2000)]

    @router.get("/payment-health", response_model=MetricResponse, tags=["product"])
    async def payment_health(
        request: Request,
        response: Response,
        start: Start = None,
        end: End = None,
        region: RegionFilter = None,
        limit: Limit = 1000,
    ) -> MetricResponse:
        return await metrics("payments", request, response, start, end, region, limit)

    @router.get("/revenue", response_model=MetricResponse, tags=["product"])
    async def revenue(
        request: Request,
        response: Response,
        start: Start = None,
        end: End = None,
        region: RegionFilter = None,
        limit: Limit = 1000,
    ) -> MetricResponse:
        return await metrics("revenue", request, response, start, end, region, limit)

    @router.get("/shipment-health", response_model=MetricResponse, tags=["product"])
    async def shipment_health(
        request: Request,
        response: Response,
        start: Start = None,
        end: End = None,
        region: RegionFilter = None,
        limit: Limit = 1000,
    ) -> MetricResponse:
        return await metrics("shipments", request, response, start, end, region, limit)

    @router.get("/refund-requests", response_model=MetricResponse, tags=["product"])
    async def refund_requests(
        request: Request,
        response: Response,
        start: Start = None,
        end: End = None,
        region: RegionFilter = None,
        limit: Limit = 1000,
    ) -> MetricResponse:
        return await metrics("refunds", request, response, start, end, region, limit)

    @router.get("/operations-health", response_model=MetricResponse, tags=["product"])
    async def operations_health(
        request: Request,
        response: Response,
        start: Start = None,
        end: End = None,
        region: RegionFilter = None,
        limit: Limit = 1000,
    ) -> MetricResponse:
        return await metrics("operations", request, response, start, end, region, limit)

    @router.get("/analytics/status", response_model=QualityStatus, tags=["product"])
    async def analytics_status(request: Request) -> QualityStatus:
        try:
            return QualityStatus.model_validate(
                await quality_state(
                    request.app.state.engine, settings.analytics_stale_after_seconds
                )
            )
        except (SQLAlchemyError, TimeoutError) as exc:
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc

    @router.get("/pipeline/status", response_model=PipelineStatus, tags=["product"])
    async def pipeline_status(request: Request) -> PipelineStatus:
        try:
            quality = QualityStatus.model_validate(
                await quality_state(
                    request.app.state.engine, settings.analytics_stale_after_seconds
                )
            )
            return PipelineStatus(analytics=quality)
        except (SQLAlchemyError, TimeoutError) as exc:
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc

    @router.get("/incidents", response_model=IncidentList, tags=["incidents"])
    async def incidents(
        request: Request,
        region: RegionFilter = None,
        status: Annotated[Literal["open", "resolved"] | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[str | None, Query(max_length=256)] = None,
    ) -> IncidentList:
        try:
            rows, next_cursor = await list_incidents(
                request.app.state.engine, region, status, limit, cursor
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="invalid_cursor") from exc
        except (SQLAlchemyError, TimeoutError) as exc:
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc
        return IncidentList(
            items=[IncidentSummary.model_validate(row) for row in rows],
            next_cursor=next_cursor,
        )

    @router.get("/incidents/{incident_id}", response_model=IncidentDetail, tags=["incidents"])
    async def incident(request: Request, incident_id: UUID) -> IncidentDetail:
        try:
            item = await incident_detail(request.app.state.engine, incident_id)
        except (SQLAlchemyError, TimeoutError) as exc:
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc
        if item is None:
            raise HTTPException(status_code=404, detail="incident_not_found")
        return IncidentDetail.model_validate(item)

    return router
