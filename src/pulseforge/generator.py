"""Seeded synthetic journeys; valid business anomalies and corrupt transport records differ."""

import json
import random
from collections import deque
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pulseforge.events import STATUS_BY_TYPE, CommerceEvent, EventType

Scenario = Literal["normal", "payment-spike", "refund-spike", "shipment-delays", "large-orders"]
SCENARIOS = ("normal", "payment-spike", "refund-spike", "shipment-delays", "large-orders")


class EventGenerator:
    def __init__(self, seed: int = 42, scenario: Scenario = "normal", anomaly_rate: float = 0.02):
        if scenario not in SCENARIOS or not 0 <= anomaly_rate <= 1:
            raise ValueError("invalid scenario or anomaly rate")
        self.random = random.Random(seed)
        self.scenario = scenario
        self.anomaly_rate = anomaly_rate
        self.pending: deque[CommerceEvent] = deque()
        self.previous: bytes | None = None

    def _id(self) -> str:
        return str(UUID(int=self.random.getrandbits(128), version=4))

    def _journey(self, now: datetime) -> None:
        rng = self.random
        common = {
            "timestamp": now,
            "customer_id": f"customer-{rng.randint(1, 500):04}",
            "product_id": f"product-{rng.randint(1, 80):03}",
            "order_id": self._id(),
            "region": rng.choice(["us-east", "us-west", "eu-west", "ap-south"]),
            "metadata": {"synthetic": True, "scenario": self.scenario},
        }
        amount = Decimal(rng.randint(500, 60000)) / 100
        if self.scenario == "large-orders" and rng.random() < 0.3:
            amount *= 100
        payment_provider = rng.choice(["stripe", "adyen", "paypal"])
        shipment_provider = rng.choice(["ups", "fedex", "dhl"])

        def append(kind: EventType, **fields: object) -> None:
            self.pending.append(
                CommerceEvent.model_validate(
                    {
                        **common,
                        "event_id": self._id(),
                        "event_type": kind,
                        "status": STATUS_BY_TYPE[kind],
                        **fields,
                    }
                )
            )

        append(EventType.CUSTOMER_LOGIN, order_id=None, product_id=None)
        append(EventType.ORDER_CREATED, amount=amount)
        failure_rate = 0.65 if self.scenario == "payment-spike" else 0.03
        if rng.random() < failure_rate:
            append(EventType.PAYMENT_FAILED, amount=amount, payment_provider=payment_provider)
        else:
            append(EventType.PAYMENT_PROCESSED, amount=amount, payment_provider=payment_provider)
            append(EventType.SHIPMENT_CREATED, shipment_provider=shipment_provider)
            delay_rate = 0.65 if self.scenario == "shipment-delays" else 0.04
            if rng.random() < delay_rate:
                append(EventType.SHIPMENT_DELAYED, shipment_provider=shipment_provider)
            refund_rate = 0.5 if self.scenario == "refund-spike" else 0.02
            if rng.random() < refund_rate:
                append(EventType.REFUND_REQUESTED, amount=amount)
        append(
            EventType.INVENTORY_UPDATED,
            customer_id=None,
            order_id=None,
            metadata={**common["metadata"], "quantity": rng.randint(0, 1000)},
        )

    def next_event(self, now: datetime | None = None) -> CommerceEvent:
        now = now or datetime.now(UTC)
        if not self.pending:
            self._journey(now)
        event = self.pending.popleft()
        return CommerceEvent.model_validate({**event.model_dump(), "timestamp": now})

    def next_record(self) -> tuple[bytes, bytes]:
        event = self.next_event()
        payload = event.model_dump_json().encode()
        if self.random.random() < self.anomaly_rate:
            corruption = self.random.choice(["duplicate", "malformed", "missing"])
            if corruption == "duplicate" and self.previous:
                payload = self.previous
            elif corruption == "malformed":
                payload = b'{"schema_version":1,"broken":'
            else:
                record = event.model_dump(mode="json")
                record.pop("event_id")
                payload = json.dumps(record).encode()
        else:
            self.previous = payload
        # Keep duplicate records on the same partition; malformed records have an explicit key.
        try:
            record = json.loads(payload)
            key = record.get("order_id") or record.get("customer_id") or record.get("product_id")
        except json.JSONDecodeError:
            key = "invalid"
        return str(key).encode(), payload
