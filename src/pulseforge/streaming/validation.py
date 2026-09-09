"""Executor-safe validation. Diagnostics never interpolate untrusted record values."""

import json
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from pulseforge.events import CommerceEvent, EventType

DETAILS = {
    "malformed_json": "Payload is not valid UTF-8 JSON.",
    "missing_event_id": "Required event_id is absent or null.",
    "invalid_event_type": "event_type is not supported by contract v1.",
    "schema_validation": "Payload does not match contract v1 shape or field types.",
    "invalid_business_fields": "Required business fields or business rules failed.",
    "other_validation": "Payload cannot be validated as a commerce object.",
}


def reject_constant(value: str) -> None:
    raise ValueError("Non-finite JSON number")


def validate_payload(payload: bytes | None, ingested_at: datetime) -> dict[str, Any]:
    def invalid(code: str) -> dict[str, Any]:
        return {"event": None, "error_code": code, "error_detail": DETAILS[code]}

    try:
        record = (
            json.loads(bytes(payload), parse_constant=reject_constant)
            if payload is not None
            else None
        )
    except (UnicodeDecodeError, ValueError, RecursionError):
        return invalid("malformed_json")
    if not isinstance(record, dict):
        return invalid("other_validation")
    if record.get("event_id") is None:
        return invalid("missing_event_id")
    kind = record.get("event_type")
    if not isinstance(kind, str) or kind not in {kind.value for kind in EventType}:
        return invalid("invalid_event_type")
    try:
        event = CommerceEvent.model_validate(
            record, context={"reference_time": ingested_at.replace(tzinfo=UTC)}
        )
    except ValidationError as exc:
        errors = exc.errors(include_input=False, include_context=False, include_url=False)
        business_fields = {
            "amount",
            "status",
            "customer_id",
            "order_id",
            "product_id",
            "timestamp",
            "payment_provider",
            "shipment_provider",
            "region",
            "metadata",
            "currency",
        }
        code = "schema_validation"
        if any(not error["loc"] or error["loc"][0] in business_fields for error in errors):
            code = "invalid_business_fields"
        return invalid(code)
    normalized = event.model_dump(mode="python")
    normalized["event_id"] = str(event.event_id)
    normalized["event_type"] = event.event_type.value
    normalized["event_ts"] = normalized.pop("timestamp").astimezone(UTC).replace(tzinfo=None)
    normalized["metadata"] = json.dumps(normalized["metadata"], sort_keys=True)
    return {"event": normalized, "error_code": None, "error_detail": None}
