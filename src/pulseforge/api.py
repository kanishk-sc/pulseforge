import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from redis.asyncio import Redis, from_url
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from pulseforge.analytics import (
    AnalyticsOverview,
    PipelineStatus,
    fetch_overview,
    fetch_pipeline_status,
)
from pulseforge.config import Settings
from pulseforge.dependencies import check_dependencies
from pulseforge.logging import configure_logging
from pulseforge.product.api import create_product_router
from pulseforge.telemetry import (
    ANALYTICS_QUERY_FAILURES,
    CACHE_OPERATIONS,
    DEPENDENCY_FAILURES,
    configure_tracing,
    failure_reason,
    observe_request,
    record_counter,
    trace_operation,
)

logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    service: str = "pulseforge-api"
    version: str = "0.1.0"
    dependencies: dict[str, str] = Field(default_factory=dict)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging()
        app.state.engine = create_async_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
            connect_args={"timeout": 3},
        )
        app.state.redis = from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
        )
        try:
            yield
        finally:
            await app.state.redis.aclose()
            await app.state.engine.dispose()
            if hasattr(app.state, "trace_provider"):
                app.state.trace_provider.shutdown()

    app = FastAPI(title="PulseForge API", version="0.1.0", lifespan=lifespan)
    app.include_router(create_product_router(settings))
    app.middleware("http")(observe_request)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        candidate = request.headers.get("x-request-id", "")
        request_id = candidate if re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", candidate) else str(uuid4())
        started = time.monotonic()
        with app.state.tracer.start_as_current_span("http.request", kind=SpanKind.SERVER) as span:
            try:
                response = await call_next(request)
            except Exception:
                logger.exception("request_failed", extra={"request_id": request_id})
                response = JSONResponse(
                    status_code=500, content={"error": "internal_error", "request_id": request_id}
                )
            route = request.scope.get("route")
            route_path = getattr(route, "path", "unmatched")
            span.update_name(f"{request.method} {route_path}")
            span.set_attribute("http.request.method", request.method)
            span.set_attribute("http.route", route_path)
            span.set_attribute("http.response.status_code", response.status_code)
            if response.status_code >= 500:
                span.set_status(Status(StatusCode.ERROR))
            response.headers["X-Request-ID"] = request_id
            logger.info(
                "http_request",
                extra={
                    "request_id": request_id,
                    "path": route_path,
                    "status": response.status_code,
                    "duration_ms": round((time.monotonic() - started) * 1000, 2),
                },
            )
        return response

    @app.get("/health", response_model=HealthResponse, tags=["health"])
    async def health() -> HealthResponse:
        """Liveness is independent of external dependencies."""
        return HealthResponse(status="ok")

    @app.get(
        "/ready",
        response_model=HealthResponse,
        tags=["health"],
        responses={503: {"model": HealthResponse}},
    )
    async def ready(request: Request):
        dependencies = await check_dependencies(settings, request.app.state.engine)
        healthy = all(value == "up" for value in dependencies.values())
        result = HealthResponse(status="ok" if healthy else "degraded", dependencies=dependencies)
        return JSONResponse(result.model_dump(), status_code=200 if healthy else 503)

    async def cached_overview(
        redis_client: Redis, engine, hours: int
    ) -> tuple[AnalyticsOverview, str]:
        cache_key = f"analytics:overview:v1:{hours}"
        cache_available = True
        try:
            with trace_operation("redis.get"):
                async with asyncio.timeout(0.5):
                    cached = await redis_client.get(cache_key)
            if cached:
                record_counter(CACHE_OPERATIONS, "get", "hit")
                return AnalyticsOverview.model_validate_json(cached), "hit"
            record_counter(CACHE_OPERATIONS, "get", "miss")
        except (RedisError, TimeoutError) as exc:
            cache_available = False
            reason = failure_reason(exc)
            record_counter(CACHE_OPERATIONS, "get", reason)
            record_counter(DEPENDENCY_FAILURES, "redis", reason)

        try:
            with trace_operation("postgres.analytics_overview"):
                overview = await fetch_overview(engine, hours)
        except (SQLAlchemyError, TimeoutError) as exc:
            record_counter(ANALYTICS_QUERY_FAILURES)
            record_counter(DEPENDENCY_FAILURES, "postgres", failure_reason(exc))
            logger.warning("analytics_query_failed", extra={"error_type": type(exc).__name__})
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc
        if not cache_available:
            return overview, "bypass"
        try:
            with trace_operation("redis.set"):
                async with asyncio.timeout(0.5):
                    await redis_client.setex(
                        cache_key,
                        settings.analytics_cache_ttl_seconds,
                        overview.model_dump_json(),
                    )
            record_counter(CACHE_OPERATIONS, "set", "stored")
        except (RedisError, TimeoutError) as exc:
            reason = failure_reason(exc)
            record_counter(CACHE_OPERATIONS, "set", reason)
            record_counter(DEPENDENCY_FAILURES, "redis", reason)
        return overview, "miss"

    @app.get(
        "/api/metrics/overview",
        response_model=AnalyticsOverview,
        tags=["analytics"],
    )
    async def analytics_overview(
        request: Request,
        response: Response,
        hours: int = Query(default=24, ge=1, le=168),
    ) -> AnalyticsOverview:
        overview, cache_status = await cached_overview(
            request.app.state.redis, request.app.state.engine, hours
        )
        response.headers["X-Cache"] = cache_status
        return overview

    @app.get(
        "/api/pipeline/status",
        response_model=PipelineStatus,
        tags=["analytics"],
    )
    async def pipeline_status(request: Request) -> PipelineStatus:
        try:
            with trace_operation("postgres.pipeline_status"):
                return await fetch_pipeline_status(request.app.state.engine)
        except (SQLAlchemyError, TimeoutError) as exc:
            record_counter(ANALYTICS_QUERY_FAILURES)
            record_counter(DEPENDENCY_FAILURES, "postgres", failure_reason(exc))
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    configure_tracing(app, settings)

    return app


app = create_app()
