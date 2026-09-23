"""Low-cardinality and failure-isolation contracts for operational telemetry."""

import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi.testclient import TestClient
from prometheus_client import generate_latest
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from pulseforge.api import create_app
from pulseforge.logging import JsonFormatter
from pulseforge.ops_exporter import durable_incident_count
from pulseforge.product.operational_runs import record_dbt_quality
from pulseforge.streaming.__main__ import terminated_critical_query
from pulseforge.streaming.progress import ProgressLogger
from pulseforge.streaming.queries import start_validation_telemetry
from pulseforge.telemetry import failure_reason

ROOT = Path(__file__).resolve().parents[1]


def test_timeout_reason_includes_redis_and_pool_deadlines():
    assert failure_reason(TimeoutError()) == "timeout"
    assert failure_reason(RedisTimeoutError()) == "timeout"
    assert failure_reason(SQLAlchemyTimeoutError()) == "timeout"
    assert failure_reason(RuntimeError()) == "error"


def test_request_metrics_use_route_template_and_bounded_status_class():
    with TestClient(create_app()) as client:
        client.get("/health?customer_id=private")
        client.get("/not-a-real-route/secret-value")
        client.request("CUSTOMVERB", "/not-a-real-route")
        metrics = client.get("/metrics").text
    assert 'route="/health",status_class="2xx"' in metrics
    assert 'route="unmatched",status_class="4xx"' in metrics
    assert 'method="OTHER",route="unmatched",status_class="4xx"' in metrics
    assert "CUSTOMVERB" not in metrics
    assert "private" not in metrics
    assert "secret-value" not in metrics
    assert "customer_id" not in metrics
    assert 'route="/metrics"' not in metrics


def test_api_telemetry_collector_failure_cannot_break_liveness(monkeypatch):
    class BrokenMetric:
        def labels(self, *_args):
            raise RuntimeError("telemetry unavailable")

    monkeypatch.setattr("pulseforge.telemetry.HTTP_REQUESTS", BrokenMetric())
    with TestClient(create_app()) as client:
        response = client.get("/health")
    assert response.status_code == 200


def test_readiness_failure_metric_uses_dependency_enum(monkeypatch):
    monkeypatch.setattr(
        "pulseforge.api.check_dependencies",
        AsyncMock(return_value={"postgres": "down", "kafka": "up", "object_storage": "up"}),
    )
    before = generate_latest().decode()
    with TestClient(create_app()) as client:
        assert client.get("/ready").status_code == 503
    after = generate_latest().decode()
    assert 'route="/ready",status_class="5xx"' in after
    assert after != before


def test_dashboards_use_provisioned_datasource_and_bounded_metric_names():
    dashboards = list((ROOT / "observability/grafana/dashboards").glob("*.json"))
    assert len(dashboards) >= 4
    for path in dashboards:
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        assert dashboard["panels"]
        assert dashboard["uid"].startswith("pulseforge-")
        for panel in dashboard["panels"]:
            assert panel["datasource"]["uid"] == "pulseforge-prometheus"
            assert panel["description"]
            for target in panel["targets"]:
                assert "pulseforge_" in target["expr"] or "up{" in target["expr"]
                assert not any(
                    identifier in target["expr"]
                    for identifier in ("event_id", "order_id", "customer_id", "build_id")
                )


def test_stream_progress_exports_fixed_query_and_partition_labels():
    listener = ProgressLogger()
    payload = {
        "name": "raw",
        "batchId": 7,
        "numInputRows": 3,
        "processedRowsPerSecond": 1.5,
        "durationMs": {"triggerExecution": 250},
        "sources": [{"endOffset": json.dumps({"commerce.events.v1": {"0": 9, "1": 5}})}],
        "stateOperators": [{"numRowsDroppedByWatermark": 0}],
    }
    listener.onQueryProgress(SimpleNamespace(progress=SimpleNamespace(json=json.dumps(payload))))
    from pulseforge.streaming.telemetry import INPUT_ROWS, SOURCE_OFFSET

    assert INPUT_ROWS.labels("raw")._value.get() >= 3
    assert SOURCE_OFFSET.labels("0")._value.get() == 9
    assert SOURCE_OFFSET.labels("1")._value.get() == 5
    assert (
        "commerce.events.v1"
        not in generate_latest()
        .decode()
        .split("pulseforge_stream_source_offset{", 1)[1]
        .split("}", 1)[0]
    )


def test_broken_stream_metric_does_not_fail_progress_callback(monkeypatch):
    class BrokenMetric:
        def labels(self, *_args):
            raise RuntimeError("telemetry unavailable")

    monkeypatch.setattr("pulseforge.streaming.progress.LAST_PROGRESS", BrokenMetric())
    payload = {"name": "raw", "batchId": 1, "numInputRows": 0, "stateOperators": []}
    ProgressLogger().onQueryProgress(
        SimpleNamespace(progress=SimpleNamespace(json=json.dumps(payload)))
    )


def test_diagnostic_query_failure_does_not_stop_critical_ingestion():
    queries = [
        SimpleNamespace(name="raw", isActive=True),
        SimpleNamespace(name="dlq", isActive=True),
        SimpleNamespace(name="valid-events", isActive=True),
        SimpleNamespace(name="validation-counts", isActive=False),
    ]
    assert terminated_critical_query(queries) is None
    queries[2].isActive = False
    assert terminated_critical_query(queries) == "valid-events"


def test_diagnostic_query_start_failure_is_best_effort():
    class FailedWriter:
        def foreachBatch(self, _callback):
            return self

        def option(self, *_args):
            return self

        def queryName(self, _name):
            return self

        def trigger(self, **_kwargs):
            return self

        def start(self):
            raise RuntimeError("diagnostic checkpoint unavailable")

    invalid = SimpleNamespace(writeStream=FailedWriter())
    settings = SimpleNamespace(
        checkpoint=lambda _name: "s3a://diagnostics", stream_trigger="5 seconds"
    )
    assert start_validation_telemetry(invalid, settings) is None


def test_terminated_query_metric_bounds_unknown_query_name():
    from pulseforge.streaming.telemetry import QUERY_FAILURES

    listener = ProgressLogger()
    listener.onQueryStarted(SimpleNamespace(id="unknown-id", name="untrusted-query-name"))
    before = QUERY_FAILURES.labels("other")._value.get()
    listener.onQueryTerminated(SimpleNamespace(id="unknown-id", exception="failed"))
    assert QUERY_FAILURES.labels("other")._value.get() == before + 1
    assert "untrusted-query-name" not in generate_latest().decode()


def test_failed_quality_telemetry_is_best_effort(tmp_path):
    artifact = tmp_path / "run_results.json"
    artifact.write_text('{"results": []}', encoding="utf-8")

    class Connection:
        def __init__(self):
            self.rollback_count = 0

        def execute(self, *_args):
            raise AssertionError("unverified result must not be inserted")

        def rollback(self):
            self.rollback_count += 1

    connection = Connection()
    record_dbt_quality(connection, uuid4(), artifact)
    assert connection.rollback_count == 1


def test_incident_gauge_uses_business_rows_not_best_effort_run_counts():
    class Connection:
        def execute(self, query):
            assert query == "SELECT count(*) FROM product.incidents"
            return SimpleNamespace(fetchone=lambda: (2,))

    assert durable_incident_count(Connection()) == 2


def test_structured_log_does_not_format_exception_or_secret_text():
    record = logging.LogRecord("test", logging.ERROR, __file__, 1, "db unavailable", (), None)
    record.request_id = "safe-id"
    rendered = JsonFormatter().format(record)
    assert json.loads(rendered)["request_id"] == "safe-id"
    assert "password" not in rendered
