from datetime import timedelta
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg.rows import dict_row

from pulseforge.product.publication import begin_build, fail_build
from pulseforge.streaming.config import StreamSettings

pytestmark = [pytest.mark.integration, pytest.mark.product]


def connect():
    settings = StreamSettings()
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        connect_timeout=5,
        row_factory=dict_row,
    )


def test_payment_api_matches_published_postgres_values_exactly():
    with connect() as connection:
        expected = connection.execute(
            """SELECT p.*, b.dbt_invocation_id
               FROM product.payment_health_hourly p
               JOIN product.analytics_builds b USING (build_id)
               WHERE b.status='succeeded'
               ORDER BY b.published_at DESC, p.metric_hour_utc, p.region_code LIMIT 1"""
        ).fetchone()
    assert expected is not None
    response = httpx.get(
        "http://127.0.0.1:8000/api/v1/payment-health",
        params={
            "start": expected["metric_hour_utc"].isoformat(),
            "end": (expected["metric_hour_utc"] + timedelta(hours=1)).isoformat(),
            "region": expected["region_code"],
        },
        timeout=10,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["build"]["build_id"] == str(expected["build_id"])
    assert body["build"]["dbt_invocation_id"] == expected["dbt_invocation_id"]
    assert body["points"] == [
        {
            "metric_hour_utc": expected["metric_hour_utc"].isoformat().replace("+00:00", "Z"),
            "region_code": expected["region_code"],
            "payment_attempt_count": expected["payment_attempt_count"],
            "successful_payment_count": expected["successful_payment_count"],
            "failed_payment_count": expected["failed_payment_count"],
            "failure_rate": format(expected["failure_rate"], "f"),
            "attempted_amount": format(expected["attempted_amount"], "f"),
            "successful_payment_amount": format(expected["successful_payment_amount"], "f"),
            "failed_payment_amount": format(expected["failed_payment_amount"], "f"),
        }
    ]


def test_publication_metadata_and_evidence_use_the_frozen_dbt_snapshot():
    with connect() as connection:
        build = connection.execute(
            """SELECT build_id, source_max_event_ts, source_max_ingested_at, source_event_count
               FROM product.analytics_builds WHERE status='succeeded'
               ORDER BY published_at DESC, build_id DESC LIMIT 1"""
        ).fetchone()
        source = connection.execute(
            """SELECT max(event_ts) AS max_event_ts, max(ingested_at) AS max_ingested_at,
                      count(*) AS event_count
               FROM analytics_staging.stg_stream_events"""
        ).fetchone()
        missing_evaluation_times = connection.execute(
            """SELECT count(*) AS missing FROM product.analytics_evidence
               WHERE build_id=%s AND evaluation_ts IS NULL""",
            (build["build_id"],),
        ).fetchone()

    assert build["source_max_event_ts"] == source["max_event_ts"]
    assert build["source_max_ingested_at"] == source["max_ingested_at"]
    assert build["source_event_count"] == source["event_count"]
    assert missing_evaluation_times["missing"] == 0


def test_failed_build_never_replaces_latest_successful_publication():
    with connect() as connection:
        failed_build_id = begin_build(
            connection, f"product-integration-intentional-failure-{uuid4()}"
        )
        fail_build(connection, failed_build_id, "intentional integration fixture")
        builds = connection.execute(
            """SELECT build_id, status FROM product.analytics_builds
               ORDER BY started_at DESC"""
        ).fetchall()
    assert {row["status"] for row in builds} >= {"failed", "succeeded"}
    status = httpx.get("http://127.0.0.1:8000/api/v1/analytics/status", timeout=10).json()
    successful_ids = {str(row["build_id"]) for row in builds if row["status"] == "succeeded"}
    failed_ids = {str(row["build_id"]) for row in builds if row["status"] == "failed"}
    assert status["build"]["build_id"] in successful_ids
    assert status["build"]["build_id"] not in failed_ids
    assert status["latest_failed_build_at"] is not None


def test_detected_incident_and_evidence_are_served_exactly():
    with connect() as connection:
        expected = connection.execute(
            """SELECT i.*, count(e.source_event_id) AS evidence_count
               FROM product.incidents i
               JOIN product.incident_evidence e USING (incident_id)
               WHERE detector_name='payment_failure_rate_increase' AND region_code='ap-south'
                 AND status='open'
               GROUP BY i.incident_id ORDER BY detected_at DESC LIMIT 1"""
        ).fetchone()
    assert expected is not None
    listing = httpx.get(
        "http://127.0.0.1:8000/api/v1/incidents",
        params={"region": "ap-south", "status": "open"},
        timeout=10,
    )
    assert listing.status_code == 200
    assert str(expected["incident_id"]) in {item["incident_id"] for item in listing.json()["items"]}
    detail = httpx.get(
        f"http://127.0.0.1:8000/api/v1/incidents/{expected['incident_id']}", timeout=10
    )
    assert detail.status_code == 200
    body = detail.json()
    assert body["observed_metric"] == format(expected["observed_metric"], "f")
    assert body["baseline_metric"] == format(expected["baseline_metric"], "f")
    assert len(body["evidence"]) == expected["evidence_count"] == 20
