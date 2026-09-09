import logging
import re
import time
from contextlib import asynccontextmanager
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import create_async_engine

from pulseforge.config import Settings
from pulseforge.dependencies import check_dependencies
from pulseforge.logging import configure_logging

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
        try:
            yield
        finally:
            await app.state.engine.dispose()

    app = FastAPI(title="PulseForge API", version="0.1.0", lifespan=lifespan)

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

    return app


app = create_app()
