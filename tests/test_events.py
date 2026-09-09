from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from pulseforge.events import CommerceEvent, EventType
from pulseforge.generator import EventGenerator


@pytest.fixture
def order():
    generator = EventGenerator(anomaly_rate=0)
    generator.next_event()
    return generator.next_event().model_dump(mode="json")


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_id", None),
        ("event_id", "bad-uuid"),
        ("customer_id", ""),
        ("customer_id", None),
        ("order_id", None),
        ("product_id", None),
        ("amount", "-1.00"),
        ("amount", "1.001"),
        ("amount", "NaN"),
        ("amount", None),
        ("event_type", "unknown"),
        ("schema_version", 2),
        ("status", "paid"),
        ("region", "unknown"),
        ("currency", "EUR"),
        ("timestamp", "2026-01-01T12:00:00"),
        ("timestamp", "1900-01-01T00:00:00Z"),
        ("timestamp", (datetime.now(UTC) + timedelta(days=1)).isoformat()),
    ],
)
def test_reject_invalid_contract(order, field, value):
    order[field] = value
    with pytest.raises(ValidationError):
        CommerceEvent.model_validate(order)


def test_reject_unknown_field(order):
    with pytest.raises(ValidationError):
        CommerceEvent.model_validate({**order, "secret_extra": "not accepted"})


def test_serialization_preserves_money(order):
    order["amount"] = "123.45"
    event = CommerceEvent.model_validate(order)
    assert CommerceEvent.model_validate_json(event.model_dump_json()) == event
    assert event.model_dump(mode="json")["amount"] == "123.45"


@pytest.mark.parametrize(
    "kind,field,value",
    [
        (EventType.PAYMENT_PROCESSED, "payment_provider", None),
        (EventType.SHIPMENT_CREATED, "shipment_provider", None),
        (EventType.INVENTORY_UPDATED, "metadata", {"quantity": -1}),
        (EventType.INVENTORY_UPDATED, "metadata", {"quantity": True}),
        (EventType.INVENTORY_UPDATED, "metadata", {}),
    ],
)
def test_type_specific_requirements(kind, field, value):
    generator = EventGenerator(anomaly_rate=0)
    for _ in range(1000):
        event = generator.next_event()
        if event.event_type == kind:
            with pytest.raises(ValidationError):
                CommerceEvent.model_validate({**event.model_dump(), field: value})
            return
    pytest.fail(f"generator did not emit {kind}")
