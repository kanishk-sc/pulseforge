import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Histogram

from pulseforge.config import Settings

HTTP_REQUESTS = Counter(
    "pulseforge_http_requests_total",
    "HTTP requests handled by the analytics API.",
    ("method", "route", "status"),
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
        HTTP_REQUESTS.labels(request.method, route_path, str(response_status)).inc()
        HTTP_DURATION.labels(request.method, route_path).observe(time.perf_counter() - started)


def configure_tracing(app: FastAPI, settings: Settings) -> None:
    provider = TracerProvider(resource=Resource.create({"service.name": "pulseforge-api"}))
    if settings.otel_exporter_otlp_endpoint:
        endpoint = settings.otel_exporter_otlp_endpoint.rstrip("/") + "/v1/traces"
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
