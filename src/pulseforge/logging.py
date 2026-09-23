import json
import logging
from datetime import UTC, datetime

from opentelemetry import trace


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "request_id",
            "path",
            "status",
            "duration_ms",
            "event_id",
            "error_type",
            "stage",
        ):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            payload["trace_id"] = f"{span_context.trace_id:032x}"
            payload["span_id"] = f"{span_context.span_id:016x}"
        # Do not serialize exception messages: connection errors may contain credentials.
        if record.exc_info and record.exc_info[0]:
            payload["error_type"] = record.exc_info[0].__name__
        return json.dumps(payload)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
