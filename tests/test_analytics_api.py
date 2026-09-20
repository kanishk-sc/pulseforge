from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import RedisError

from pulseforge.analytics import AnalyticsOverview, PipelineStatus
from pulseforge.api import create_app


@pytest.fixture
def client():
    with TestClient(create_app()) as test_client:
        yield test_client


class FakeRedis:
    def __init__(self):
        self.values: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        assert ttl == 60
        self.values[key] = value

    async def aclose(self) -> None:
        return None


class UnavailableRedis(FakeRedis):
    async def get(self, key: str) -> str | None:
        raise RedisError("cache offline")

    async def setex(self, key: str, ttl: int, value: str) -> None:
        raise RedisError("cache offline")


def overview() -> AnalyticsOverview:
    now = datetime.now(UTC)
    return AnalyticsOverview(
        generated_at=now,
        hours=24,
        revenue=[
            {
                "metric_hour": now,
                "successful_payment_count": 2,
                "successful_revenue": 125.5,
            }
        ],
        payments=[],
        shipments=[],
        refunds=[],
        operations=[],
        quality=[],
    )


def test_overview_is_typed_and_cached(client, monkeypatch):
    fake_redis = FakeRedis()
    client.app.state.redis = fake_redis
    fetch = AsyncMock(return_value=overview())
    monkeypatch.setattr("pulseforge.api.fetch_overview", fetch)

    first = client.get("/api/metrics/overview")
    second = client.get("/api/metrics/overview")

    assert first.status_code == 200
    assert first.headers["X-Cache"] == "miss"
    assert second.headers["X-Cache"] == "hit"
    assert second.json()["revenue"][0]["successful_revenue"] == 125.5
    fetch.assert_awaited_once()


def test_redis_failure_degrades_to_warehouse(client, monkeypatch):
    client.app.state.redis = UnavailableRedis()
    monkeypatch.setattr("pulseforge.api.fetch_overview", AsyncMock(return_value=overview()))

    response = client.get("/api/metrics/overview")

    assert response.status_code == 200
    assert response.headers["X-Cache"] == "miss"


def test_pipeline_status_and_prometheus_metrics(client, monkeypatch):
    monkeypatch.setattr(
        "pulseforge.api.fetch_pipeline_status",
        AsyncMock(
            return_value=PipelineStatus(
                status="fresh",
                warehouse_updated_at=datetime.now(UTC),
                minutes_since_update=1.2,
            )
        ),
    )

    status = client.get("/api/pipeline/status")
    metrics = client.get("/metrics")

    assert status.json()["status"] == "fresh"
    assert "pulseforge_http_requests_total" in metrics.text
    assert 'route="/api/pipeline/status"' in metrics.text
