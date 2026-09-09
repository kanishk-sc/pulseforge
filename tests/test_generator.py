import json
from collections import Counter
from datetime import UTC, datetime

from pydantic import ValidationError

from pulseforge.events import CommerceEvent, EventType
from pulseforge.generator import EventGenerator


def test_seeded_generation_is_reproducible_and_covers_contract():
    now = datetime.now(UTC)
    left, right = EventGenerator(), EventGenerator()
    first = [left.next_event(now) for _ in range(3000)]
    assert first == [right.next_event(now) for _ in range(3000)]
    assert {event.event_type for event in first} == set(EventType)
    assert len({event.event_id for event in first}) == len(first)


def test_journeys_preserve_identity_and_only_ship_paid_orders():
    generator = EventGenerator()
    orders, paid = {}, set()
    for _ in range(1000):
        event = generator.next_event()
        if event.event_type == EventType.ORDER_CREATED:
            orders[event.order_id] = event
        elif event.order_id:
            original = orders[event.order_id]
            assert (event.customer_id, event.product_id, event.region) == (
                original.customer_id,
                original.product_id,
                original.region,
            )
            if event.amount is not None:
                assert event.amount == original.amount
            if event.event_type == EventType.PAYMENT_PROCESSED:
                paid.add(event.order_id)
            if event.event_type in {EventType.SHIPMENT_CREATED, EventType.REFUND_REQUESTED}:
                assert event.order_id in paid


def test_payment_scenario_materially_increases_failure_rate():
    def rate(scenario):
        generator = EventGenerator(scenario=scenario)
        counts = Counter(generator.next_event().event_type for _ in range(3000))
        return counts[EventType.PAYMENT_FAILED] / (
            counts[EventType.PAYMENT_FAILED] + counts[EventType.PAYMENT_PROCESSED]
        )

    assert rate("normal") < 0.1
    assert rate("payment-spike") > 0.5


def test_corruption_emits_duplicates_malformed_and_missing_ids():
    generator = EventGenerator(anomaly_rate=0)
    original_key, original = generator.next_record()
    generator.anomaly_rate = 1
    duplicate = malformed = missing = False
    for _ in range(100):
        key, payload = generator.next_record()
        if payload == original:
            duplicate = True
            assert key == original_key
        else:
            try:
                record = json.loads(payload)
                missing |= "event_id" not in record
            except json.JSONDecodeError:
                malformed = True
            try:
                CommerceEvent.model_validate_json(payload)
                raise AssertionError("corrupt record unexpectedly valid")
            except ValidationError:
                pass
    assert duplicate and malformed and missing
