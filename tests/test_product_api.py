from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import RedisError
from sqlalchemy.exc import OperationalError

from pulseforge.api import create_app
from pulseforge.product.models import BuildContext


class FakeRedis:
    def __init__(self, available: bool = True):
        self.available = available
        self.values: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        if not self.available:
            raise RedisError("offline")
        return self.values.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        if not self.available:
            raise RedisError("offline")
        self.values[key] = value

    async def aclose(self) -> None:
        return None


class TimeoutRedis(FakeRedis):
    async def get(self, key: str) -> str | None:
        raise TimeoutError("cache timed out")

    async def setex(self, key: str, ttl: int, value: str) -> None:
        raise TimeoutError("cache timed out")


@pytest.fixture
def client():
    with TestClient(create_app()) as test_client:
        test_client.app.state.redis = FakeRedis()
        yield test_client


def build(state: str = "fresh") -> BuildContext:
    now = datetime.now(UTC)
    return BuildContext(
        build_id=UUID("00000000-0000-0000-0000-000000000001"),
        dbt_invocation_id="dbt-test",
        published_at=now,
        source_max_event_ts=now - timedelta(minutes=2),
        source_max_ingested_at=now - timedelta(minutes=1),
        source_event_count=42,
        age_seconds=60,
        state=state,
    )


def payment_row() -> dict:
    return {
        "build_id": build().build_id,
        "metric_hour_utc": datetime(2026, 9, 20, 12, tzinfo=UTC),
        "region_code": "us-east",
        "payment_attempt_count": 4,
        "successful_payment_count": 3,
        "failed_payment_count": 1,
        "failure_rate": Decimal("0.250000"),
        "attempted_amount": Decimal("100.10"),
        "successful_payment_amount": Decimal("75.05"),
        "failed_payment_amount": Decimal("25.05"),
    }


def test_payment_slice_preserves_decimal_strings_and_cache_identity(client, monkeypatch):
    latest = AsyncMock(return_value=build())
    rows = AsyncMock(return_value=[payment_row()])
    monkeypatch.setattr("pulseforge.product.api.latest_build", latest)
    monkeypatch.setattr("pulseforge.product.api.metric_rows", rows)
    params = {
        "start": "2026-09-20T12:00:00Z",
        "end": "2026-09-20T13:00:00Z",
        "region": "us-east",
    }

    first = client.get("/api/v1/payment-health", params=params)
    second = client.get("/api/v1/payment-health", params=params)

    assert first.status_code == 200
    assert first.headers["X-Cache"] == "miss"
    assert second.headers["X-Cache"] == "hit"
    assert first.json()["interval_semantics"] == "[start,end)"
    assert first.json()["points"][0]["attempted_amount"] == "100.10"
    rows.assert_awaited_once()


def test_cache_hit_recomputes_time_sensitive_build_state(client, monkeypatch):
    fresh = build("fresh")
    stale = build("stale").model_copy(update={"age_seconds": 9000})
    latest = AsyncMock(side_effect=[fresh, stale])
    rows = AsyncMock(return_value=[payment_row()])
    monkeypatch.setattr("pulseforge.product.api.latest_build", latest)
    monkeypatch.setattr("pulseforge.product.api.metric_rows", rows)
    params = {
        "start": "2026-09-20T12:00:00Z",
        "end": "2026-09-20T13:00:00Z",
        "region": "us-east",
    }

    assert client.get("/api/v1/payment-health", params=params).json()["state"] == "fresh"
    cached = client.get("/api/v1/payment-health", params=params)

    assert cached.headers["X-Cache"] == "hit"
    assert cached.json()["state"] == "stale"
    assert cached.json()["build"]["age_seconds"] == 9000
    rows.assert_awaited_once()


def test_utc_boundaries_and_range_validation(client, monkeypatch):
    monkeypatch.setattr("pulseforge.product.api.latest_build", AsyncMock(return_value=build()))
    monkeypatch.setattr("pulseforge.product.api.metric_rows", AsyncMock(return_value=[]))

    naive = client.get(
        "/api/v1/revenue",
        params={"start": "2026-09-20T12:00:00", "end": "2026-09-20T13:00:00Z"},
    )
    reversed_range = client.get(
        "/api/v1/revenue",
        params={"start": "2026-09-20T14:00:00Z", "end": "2026-09-20T13:00:00Z"},
    )
    oversized = client.get(
        "/api/v1/revenue",
        params={"start": "2026-07-01T00:00:00Z", "end": "2026-09-20T13:00:00Z"},
    )

    assert naive.status_code == 422
    assert reversed_range.status_code == 422
    assert oversized.status_code == 422


def test_stale_empty_and_redis_outage_are_explicit(client, monkeypatch):
    client.app.state.redis = FakeRedis(available=False)
    monkeypatch.setattr(
        "pulseforge.product.api.latest_build", AsyncMock(return_value=build("stale"))
    )
    monkeypatch.setattr("pulseforge.product.api.metric_rows", AsyncMock(return_value=[]))

    response = client.get("/api/v1/refund-requests")

    assert response.status_code == 200
    assert response.headers["X-Cache"] == "bypass"
    assert response.json()["state"] == "empty"
    assert response.json()["build"]["state"] == "stale"


def test_redis_timeout_falls_back_to_database(client, monkeypatch):
    client.app.state.redis = TimeoutRedis()
    monkeypatch.setattr("pulseforge.product.api.latest_build", AsyncMock(return_value=build()))
    rows = AsyncMock(return_value=[payment_row()])
    monkeypatch.setattr("pulseforge.product.api.metric_rows", rows)

    response = client.get("/api/v1/payment-health")

    assert response.status_code == 200
    assert response.headers["X-Cache"] == "bypass"
    assert response.json()["points"][0]["payment_attempt_count"] == 4
    rows.assert_awaited_once()


def test_unpublished_and_database_unavailable_are_not_fabricated(client, monkeypatch):
    monkeypatch.setattr("pulseforge.product.api.latest_build", AsyncMock(return_value=None))
    unpublished = client.get("/api/v1/operations-health")
    assert unpublished.status_code == 503
    assert unpublished.json()["detail"] == "analytics_publication_unavailable"

    monkeypatch.setattr(
        "pulseforge.product.api.latest_build",
        AsyncMock(side_effect=OperationalError("select", {}, Exception("offline"))),
    )
    unavailable = client.get("/api/v1/operations-health")
    assert unavailable.status_code == 503
    assert unavailable.json()["detail"] == "analytics_warehouse_unavailable"


def test_region_and_limit_validation(client):
    assert client.get("/api/v1/revenue", params={"region": "moon"}).status_code == 422
    assert client.get("/api/v1/revenue", params={"limit": 2001}).status_code == 422
    assert client.get("/api/v1/incidents", params={"status": "unknown"}).status_code == 422


def test_incident_list_exposes_public_fields_and_status_filter(client, monkeypatch):
    incident = {
        "incident_id": UUID("00000000-0000-0000-0000-000000000099"),
        "detector_name": "payment_failure_rate_increase",
        "detector_version": "1.0.0",
        "region_code": "ap-south",
        "evaluation_start": datetime(2026, 9, 20, 11, tzinfo=UTC),
        "evaluation_end": datetime(2026, 9, 20, 12, tzinfo=UTC),
        "observed_metric": Decimal("1.000000"),
        "observed_denominator": 20,
        "baseline_metric": Decimal("0.000000"),
        "threshold": Decimal("0.100000"),
        "severity": "critical",
        "summary": "A deterministic payment incident",
        "analytics_build_id": build().build_id,
        "detected_at": datetime(2026, 9, 20, 12, 1, tzinfo=UTC),
        "status": "open",
        "resolved_at": None,
    }
    listing = AsyncMock(return_value=([incident], None))
    monkeypatch.setattr("pulseforge.product.api.list_incidents", listing)

    response = client.get("/api/v1/incidents", params={"status": "open", "region": "ap-south"})

    assert response.status_code == 200
    assert response.json()["items"][0]["observed_metric"] == "1.000000"
    listing.assert_awaited_once_with(client.app.state.engine, "ap-south", "open", 50, None)
