import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from functools import wraps

from fastapi import FastAPI, Request, Response
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from prometheus_client import Counter, Histogram
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from pulseforge.config import Settings

logger = logging.getLogger(__name__)
_metric_failure_logged = False

HTTP_REQUESTS = Counter(
    "pulseforge_http_requests_total",
    "HTTP requests handled by the analytics API.",
    ("method", "route", "status_class"),
)
HTTP_DURATION = Histogram(
    "pulseforge_http_request_duration_seconds",
    "HTTP request duration in seconds.",
    ("method", "route"),
)
CACHE_OPERATIONS = Counter(
    "pulseforge_cache_operations_total",
    "Analytics cache operations by bounded result.",
    ("operation", "result"),
)
ANALYTICS_QUERY_FAILURES = Counter(
    "pulseforge_analytics_query_failures_total",
    "Failed analytics warehouse queries.",
)
DEPENDENCY_FAILURES = Counter(
    "pulseforge_dependency_failures_total",
    "Failed dependency operations; a missing series does not imply availability.",
    ("dependency", "reason"),
)


def record_counter(counter: Counter, *labels: str, amount: float = 1) -> None:
    """Best-effort diagnostic increment; a broken collector cannot change business flow."""
    global _metric_failure_logged
    try:
        (counter.labels(*labels) if labels else counter).inc(amount)
    except Exception as exc:
        if not _metric_failure_logged:
            logger.warning("metric_record_failed", extra={"error_type": type(exc).__name__})
            _metric_failure_logged = True


def failure_reason(exc: Exception) -> str:
    return (
        "timeout"
        if isinstance(exc, (TimeoutError, RedisTimeoutError, SQLAlchemyTimeoutError))
        else "error"
    )


@contextmanager
def trace_operation(name: str):
    """Create a child span with a constant name and no SQL, keys or payload attributes."""
    with trace.get_tracer("pulseforge.operations").start_as_current_span(name):
        yield


def traced_async(name: str):
    """Trace a bounded internal operation without recording arguments or return values."""

    def decorate(function):
        @wraps(function)
        async def wrapper(*args, **kwargs):
            with trace_operation(name):
                return await function(*args, **kwargs)

        return wrapper

    return decorate


async def observe_request(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    started = time.perf_counter()
    response_status = 500
    try:
        response = await call_next(request)
        response_status = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        route_path = getattr(route, "path", "unmatched")
        if route_path != "/metrics":
            try:
                status_class = f"{min(response_status // 100, 5)}xx"
                record_counter(HTTP_REQUESTS, request.method, route_path, status_class)
                HTTP_DURATION.labels(request.method, route_path).observe(
                    time.perf_counter() - started
                )
            except Exception:
                # A diagnostic exporter must never replace the business response.
                pass


def configure_tracing(app: FastAPI, settings: Settings) -> None:
    if not settings.otel_exporter_otlp_endpoint:
        app.state.tracer = trace.get_tracer("pulseforge.api")
        return
    provider = TracerProvider(
        resource=Resource.create({"service.name": "pulseforge-api"}),
        sampler=ParentBased(TraceIdRatioBased(settings.otel_trace_sample_ratio)),
    )
    endpoint = settings.otel_exporter_otlp_endpoint.rstrip("/") + "/v1/traces"
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=endpoint, timeout=0.5),
            max_queue_size=512,
            max_export_batch_size=64,
            schedule_delay_millis=1000,
            export_timeout_millis=500,
        )
    )
    trace.set_tracer_provider(provider)
    app.state.tracer = provider.get_tracer("pulseforge.api")
    app.state.trace_provider = provider
