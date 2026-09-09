"""Version one commerce contract. Invalid simulation records bypass this model explicitly."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, ValidationInfo, model_validator


class EventType(StrEnum):
    ORDER_CREATED = "order_created"
    PAYMENT_PROCESSED = "payment_processed"
    PAYMENT_FAILED = "payment_failed"
    SHIPMENT_CREATED = "shipment_created"
    SHIPMENT_DELAYED = "shipment_delayed"
    REFUND_REQUESTED = "refund_requested"
    INVENTORY_UPDATED = "inventory_updated"
    CUSTOMER_LOGIN = "customer_login"


STATUS_BY_TYPE = {
    EventType.ORDER_CREATED: "created",
    EventType.PAYMENT_PROCESSED: "paid",
    EventType.PAYMENT_FAILED: "failed",
    EventType.SHIPMENT_CREATED: "shipped",
    EventType.SHIPMENT_DELAYED: "delayed",
    EventType.REFUND_REQUESTED: "requested",
    EventType.INVENTORY_UPDATED: "updated",
    EventType.CUSTOMER_LOGIN: "authenticated",
}
MONEY_TYPES = {
    EventType.ORDER_CREATED,
    EventType.PAYMENT_PROCESSED,
    EventType.PAYMENT_FAILED,
    EventType.REFUND_REQUESTED,
}
SHIPMENT_TYPES = {EventType.SHIPMENT_CREATED, EventType.SHIPMENT_DELAYED}
Identifier = Annotated[str, Field(min_length=1, max_length=100, pattern=r"^\S+$")]


class CommerceEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    event_id: UUID
    event_type: EventType
    timestamp: datetime
    customer_id: Identifier | None = None
    order_id: Identifier | None = None
    product_id: Identifier | None = None
    amount: Decimal | None = Field(default=None, ge=0, max_digits=14, decimal_places=2)
    currency: Literal["USD"] = "USD"
    payment_provider: Literal["stripe", "adyen", "paypal"] | None = None
    shipment_provider: Literal["ups", "fedex", "dhl"] | None = None
    region: Literal["us-east", "us-west", "eu-west", "ap-south"]
    status: str
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def business_rules(self, info: ValidationInfo) -> Self:
        # Archived records validate against their original ingestion clock on replay.
        reference_time = (info.context or {}).get("reference_time") or datetime.now(UTC)
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        if (
            not datetime(2020, 1, 1, tzinfo=UTC)
            <= self.timestamp
            <= reference_time + timedelta(minutes=5)
        ):
            raise ValueError("timestamp outside supported range")
        if self.status != STATUS_BY_TYPE[self.event_type]:
            raise ValueError("status does not match event_type")
        required = []
        if self.event_type != EventType.INVENTORY_UPDATED:
            required.append("customer_id")
        if self.event_type not in {EventType.INVENTORY_UPDATED, EventType.CUSTOMER_LOGIN}:
            required.extend(["order_id", "product_id"])
        if self.event_type == EventType.INVENTORY_UPDATED:
            required.append("product_id")
            quantity = self.metadata.get("quantity")
            if type(quantity) is not int or quantity < 0:
                raise ValueError("inventory quantity must be a nonnegative integer")
        if self.event_type in MONEY_TYPES:
            required.append("amount")
        if self.event_type in {EventType.PAYMENT_PROCESSED, EventType.PAYMENT_FAILED}:
            required.append("payment_provider")
        if self.event_type in SHIPMENT_TYPES:
            required.append("shipment_provider")
        missing = [field for field in required if getattr(self, field) is None]
        if missing:
            raise ValueError(f"missing required fields: {', '.join(missing)}")
        return self
