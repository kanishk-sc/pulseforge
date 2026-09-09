from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from pulseforge.api import create_app


@pytest.fixture
def client():
    with TestClient(create_app()) as client:
        yield client


def test_liveness_does_not_need_services(client):
    response = client.get("/health", headers={"X-Request-ID": "test-123"})
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.headers["X-Request-ID"] == "test-123"


def test_untrusted_request_id_is_replaced(client):
    response = client.get("/health", headers={"X-Request-ID": "x" * 1000})
    assert len(response.headers["X-Request-ID"]) == 36


@pytest.mark.parametrize("state,code", [("up", 200), ("down", 503)])
def test_readiness_reports_dependency_failure(client, monkeypatch, state, code):
    monkeypatch.setattr(
        "pulseforge.api.check_dependencies",
        AsyncMock(return_value={"postgres": state, "kafka": "up", "object_storage": "up"}),
    )
    response = client.get("/ready")
    assert response.status_code == code
    assert response.json()["dependencies"]["postgres"] == state


def test_unexpected_exception_does_not_expose_details(client, monkeypatch):
    monkeypatch.setattr(
        "pulseforge.api.check_dependencies",
        AsyncMock(side_effect=RuntimeError("sensitive connection details")),
    )
    response = client.get("/ready")
    assert response.status_code == 500
    assert "sensitive" not in response.text
    assert response.json()["request_id"] == response.headers["X-Request-ID"]


def test_openapi_describes_health_endpoints(client):
    schema = client.get("/openapi.json").json()
    assert {"/health", "/ready"} <= schema["paths"].keys()
    assert "503" in schema["paths"]["/ready"]["get"]["responses"]
