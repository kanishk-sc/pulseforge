import logging
import re
import time
from contextlib import asynccontextmanager
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel
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
from pulseforge.telemetry import (
    ANALYTICS_QUERY_FAILURES,
    CACHE_OPERATIONS,
    configure_tracing,
    observe_request,
)

logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    service: str = "pulseforge-api"
    version: str = "0.1.0"
    dependencies: dict[str, str] = {}


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

    app = FastAPI(title="PulseForge API", version="0.1.0", lifespan=lifespan)
    app.middleware("http")(observe_request)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        candidate = request.headers.get("x-request-id", "")
        request_id = candidate if re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", candidate) else str(uuid4())
        started = time.monotonic()
        try:
            response = await call_next(request)
        except Exception:
            logger.exception("request_failed", extra={"request_id": request_id})
            response = JSONResponse(
                status_code=500, content={"error": "internal_error", "request_id": request_id}
            )
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "http_request",
            extra={
                "request_id": request_id,
                "path": request.url.path,
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
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                CACHE_OPERATIONS.labels("get", "hit").inc()
                return AnalyticsOverview.model_validate_json(cached), "hit"
            CACHE_OPERATIONS.labels("get", "miss").inc()
        except RedisError:
            CACHE_OPERATIONS.labels("get", "unavailable").inc()

        try:
            overview = await fetch_overview(engine, hours)
        except SQLAlchemyError as exc:
            ANALYTICS_QUERY_FAILURES.inc()
            logger.warning("analytics_query_failed", extra={"error_type": type(exc).__name__})
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc
        try:
            await redis_client.setex(
                cache_key,
                settings.analytics_cache_ttl_seconds,
                overview.model_dump_json(),
            )
            CACHE_OPERATIONS.labels("set", "stored").inc()
        except RedisError:
            CACHE_OPERATIONS.labels("set", "unavailable").inc()
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
            return await fetch_pipeline_status(request.app.state.engine)
        except SQLAlchemyError as exc:
            ANALYTICS_QUERY_FAILURES.inc()
            raise HTTPException(status_code=503, detail="analytics_warehouse_unavailable") from exc

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    configure_tracing(app, settings)

    return app


app = create_app()
